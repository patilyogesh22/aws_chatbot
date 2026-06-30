import os
import json
import psycopg2

from typing import Optional
from pydantic import BaseModel
from fastapi import APIRouter, Depends, HTTPException

from app.config import PG_DSN
from app.auth import get_current_user
from app.services.rag_service import retrieve, build_context
from app.services.llm_service import answer, synthesise_multi_file_answer
from app.services.structured_chat_service import answer_structured_question

router = APIRouter()


class ChatRequest(BaseModel):
    question: str
    file_name: Optional[str] = None              # old single-file support
    file_names: Optional[list[str]] = None       # new multi-file support
    top_k: Optional[int] = 5
    chat_history: Optional[list] = []


def _normalise_file_scope(req: ChatRequest) -> list[str]:
    """
    Build a clean selected-file list.

    Backward compatible:
    - old frontend sends file_name
    - new frontend sends file_names
    """
    files: list[str] = []

    if req.file_names:
        files.extend([f for f in req.file_names if f])

    if req.file_name:
        files.append(req.file_name)

    # remove duplicates but preserve order
    seen = set()
    result = []
    for f in files:
        if f not in seen:
            result.append(f)
            seen.add(f)

    return result


def _get_selected_file_types(user_id: int, file_names: list[str]) -> dict[str, str]:
    if not file_names:
        return {}

    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT file_name, file_type
                FROM app_documents
                WHERE user_id = %s
                  AND file_name = ANY(%s)
            """, (
                user_id,
                file_names,
            ))

            rows = cur.fetchall()

    return {r[0]: r[1] for r in rows}


def _save_chat_history(
    *,
    user_id: int,
    question: str,
    answer_text: str,
    file_name: str | None = None,
    file_names: list[str] | None = None,
    file_type: str | None = None,
    generated_sql: str | None = None,
    table_name: str | None = None,
    rows: list | None = None,
    columns: list | None = None,
    row_count: int | None = None,
    chat_type: str = "single",
):
    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO chat_history
                (
                    user_id,
                    question,
                    answer,
                    file_name,
                    file_names,
                    chat_type,
                    generated_sql,
                    table_name,
                    file_type,
                    result_rows,
                    result_columns,
                    row_count
                )
                VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s)
            """, (
                user_id,
                question,
                answer_text,
                file_name,
                json.dumps(file_names or []),
                chat_type,
                generated_sql,
                table_name,
                file_type,
                json.dumps(rows or []),
                json.dumps(columns or []),
                row_count,
            ))

        conn.commit()


def _answer_single_file(req: ChatRequest, user_id: int, file_name: Optional[str]):
    """
    Existing single-file behavior.
    Structured file -> NL-to-SQL.
    Unstructured/no file -> RAG.
    """
    if file_name:
        file_types = _get_selected_file_types(user_id, [file_name])
        file_type = file_types.get(file_name)

        if file_type == "structured":
            try:
                structured_response = answer_structured_question(
                    user_id=user_id,
                    file_name=file_name,
                    question=req.question,
                )

                _save_chat_history(
                    user_id=user_id,
                    question=req.question,
                    answer_text=structured_response["answer"],
                    file_name=file_name,
                    file_names=[file_name],
                    file_type="structured",
                    generated_sql=structured_response.get("sql"),
                    table_name=structured_response.get("table_name"),
                    rows=structured_response.get("rows", []),
                    columns=structured_response.get("columns", []),
                    row_count=structured_response.get(
                        "row_count",
                        len(structured_response.get("rows", []))
                    ),
                    chat_type="single",
                )

                return structured_response

            except Exception as e:
                return {
                    "answer": f"Structured file query failed: {str(e)}",
                    "chunks": [],
                    "sources": [file_name],
                    "chunks_used": 0,
                    "file_type": "structured",
                    "sql": None,
                    "table_name": None,
                    "row_count": 0,
                    "rows": [],
                    "columns": [],
                }

    chunks = retrieve(
        req.question,
        user_id=user_id,
        top_k=req.top_k,
        file_name=file_name,
    )

    if not chunks:
        return {
            "answer": "No relevant documents found for your account. Please upload a file first.",
            "chunks": [],
            "sources": [],
            "chunks_used": 0,
        }

    context = build_context(chunks)
    response = answer(req.question, context, req.chat_history)
    sources = list({c["file_name"] for c in chunks})

    _save_chat_history(
        user_id=user_id,
        question=req.question,
        answer_text=response,
        file_name=file_name,
        file_names=[file_name] if file_name else sources,
        file_type="unstructured",
        chat_type="single",
    )

    return {
        "answer": response,
        "chunks": chunks,
        "sources": sources,
        "chunks_used": len(chunks),
        "model": os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile"),
        "file_type": "unstructured",
    }


def _answer_multi_file(req: ChatRequest, user_id: int, selected_files: list[str]):
    """
    New multi-file behavior.

    - Structured files are queried one by one using NL-to-SQL.
    - Unstructured files are searched together using pgvector/RAG.
    - The final answer is synthesized by the LLM.
    """
    file_types = _get_selected_file_types(user_id, selected_files)

    missing_files = [f for f in selected_files if f not in file_types]
    if missing_files:
        return {
            "answer": f"Some selected files were not found: {', '.join(missing_files)}",
            "file_type": "multi",
            "sources": selected_files,
            "per_file": [],
            "chunks": [],
            "chunks_used": 0,
        }

    structured_files = [f for f, t in file_types.items() if t == "structured"]
    unstructured_files = [f for f, t in file_types.items() if t == "unstructured"]

    per_file_answers = []
    all_chunks = []

    # 1. Structured files: query separately
    for file_name in structured_files:
        try:
            structured_response = answer_structured_question(
                user_id=user_id,
                file_name=file_name,
                question=req.question,
            )

            per_file_answers.append({
                "file_name": file_name,
                "file_type": "structured",
                "answer": structured_response.get("answer", ""),
                "sql": structured_response.get("sql"),
                "table_name": structured_response.get("table_name"),
                "rows": structured_response.get("rows", []),
                "columns": structured_response.get("columns", []),
                "row_count": structured_response.get("row_count", 0),
            })

        except Exception as e:
            per_file_answers.append({
                "file_name": file_name,
                "file_type": "structured",
                "answer": f"Structured query failed: {str(e)}",
                "error": str(e),
                "rows": [],
                "columns": [],
                "row_count": 0,
            })

    # 2. Unstructured files: retrieve across all selected unstructured files
    if unstructured_files:
        chunks = retrieve(
            req.question,
            user_id=user_id,
            top_k=req.top_k,
            file_names=unstructured_files,
        )
        all_chunks.extend(chunks)

        if chunks:
            context = build_context(chunks)
            rag_answer = answer(req.question, context, req.chat_history)

            per_file_answers.append({
                "file_names": unstructured_files,
                "file_type": "unstructured",
                "answer": rag_answer,
                "chunks": chunks,
                "sources": list({c["file_name"] for c in chunks}),
            })
        else:
            per_file_answers.append({
                "file_names": unstructured_files,
                "file_type": "unstructured",
                "answer": "No relevant chunks were found in the selected unstructured files.",
                "chunks": [],
                "sources": unstructured_files,
            })

    if not per_file_answers:
        final_answer = "No supported selected files were found."
    elif len(per_file_answers) == 1:
        final_answer = per_file_answers[0].get("answer", "No answer found.")
    else:
        final_answer = synthesise_multi_file_answer(req.question, per_file_answers)

    _save_chat_history(
        user_id=user_id,
        question=req.question,
        answer_text=final_answer,
        file_name=None,
        file_names=selected_files,
        file_type="multi",
        chat_type="multi",
    )

    return {
        "answer": final_answer,
        "per_file": per_file_answers,
        "sources": selected_files,
        "chunks": all_chunks,
        "chunks_used": len(all_chunks),
        "file_type": "multi",
        "model": os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile"),
    }


@router.post("/chat")
def chat(
    req: ChatRequest,
    current_user: dict = Depends(get_current_user),
):
    if not req.question.strip():
        raise HTTPException(status_code=400, detail="Question cannot be empty")

    user_id = current_user["id"]
    selected_files = _normalise_file_scope(req)

    # New multi-file path
    if len(selected_files) > 1:
        return _answer_multi_file(req, user_id, selected_files)

    # Old single-file path
    selected_file = selected_files[0] if selected_files else None
    return _answer_single_file(req, user_id, selected_file)
