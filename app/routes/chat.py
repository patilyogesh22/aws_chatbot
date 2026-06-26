import os
import json
import psycopg2

from typing import Optional
from pydantic import BaseModel
from fastapi import APIRouter, Depends, HTTPException

from app.config import PG_DSN
from app.auth import get_current_user
from app.services.rag_service import retrieve, build_context
from app.services.llm_service import answer
from app.services.structured_chat_service import answer_structured_question

router = APIRouter()


class ChatRequest(BaseModel):
    question: str
    file_name: Optional[str] = None
    top_k: Optional[int] = 5
    chat_history: Optional[list] = []


@router.post("/chat")
def chat(
    req: ChatRequest,
    current_user: dict = Depends(get_current_user)
):
    if not req.question.strip():
        raise HTTPException(status_code=400, detail="Question cannot be empty")

    if req.file_name:
        with psycopg2.connect(PG_DSN) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT file_type
                    FROM app_documents
                    WHERE user_id = %s
                      AND file_name = %s
                    LIMIT 1
                """, (
                    current_user["id"],
                    req.file_name
                ))
                row = cur.fetchone()

        if row and row[0] == "structured":
            try:
                structured_response = answer_structured_question(
                    user_id=current_user["id"],
                    file_name=req.file_name,
                    question=req.question,
                )

                with psycopg2.connect(PG_DSN) as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO chat_history
                            (
                                user_id,
                                question,
                                answer,
                                file_name,
                                generated_sql,
                                table_name,
                                file_type,
                                result_rows,
                                result_columns,
                                row_count
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s)
                        """, (
                            current_user["id"],
                            req.question,
                            structured_response["answer"],
                            req.file_name,
                            structured_response.get("sql"),
                            structured_response.get("table_name"),
                            "structured",
                            json.dumps(structured_response.get("rows", [])),
                            json.dumps(structured_response.get("columns", [])),
                            structured_response.get(
                                "row_count",
                                len(structured_response.get("rows", []))
                            ),
                        ))

                    conn.commit()

                return structured_response

            except Exception as e:
                return {
                    "answer": f"Structured file query failed: {str(e)}",
                    "chunks": [],
                    "sources": [req.file_name],
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
        user_id=current_user["id"],
        top_k=req.top_k,
        file_name=req.file_name
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

    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO chat_history
                (
                    user_id,
                    question,
                    answer,
                    file_name,
                    file_type
                )
                VALUES (%s, %s, %s, %s, %s)
            """, (
                current_user["id"],
                req.question,
                response,
                req.file_name,
                "unstructured",
            ))

        conn.commit()

    sources = list({c["file_name"] for c in chunks})

    return {
        "answer": response,
        "chunks": chunks,
        "sources": sources,
        "chunks_used": len(chunks),
        "model": os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile"),
        "file_type": "unstructured"
    }