"""
main.py — FastAPI backend with Authentication + User-wise RAG + File Type Routing

Pipeline:
Login/Register
→ Upload → S3 uploads/user_{id}/
→ Detect file type
    ├── structured: CSV / XLSX / XLS / JSON
    │       → S3 + app_documents + Lambda/Glue Crawler
    │       → Future: Glue Job → RDS → dbt → SQL/NL-to-SQL
    └── unstructured: PDF / DOCX / TXT / MD / PPTX
            → S3 → Text extraction → raw_chunks with user_id
            → dbt → ChromaDB embeddings with user_id
            → user-specific RAG chat
"""

import os
import hashlib
import subprocess
from typing import Optional

import psycopg2
from fastapi import FastAPI, File, UploadFile, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from app.config import DATA_RAW_DIR, DBT_PROJECT_DIR, PG_DSN
from aws.s3_ingestion import upload_fileobj_to_s3, delete_s3_object
from app.file_classifier import classify_file

from app.auth import (
    router as auth_router,
    get_current_user,
    init_auth_tables,
)

from app.ingestion import (
    ingest_file_from_s3_key,
    delete_file_chunks,
    init_postgres,
)

from app.embeddings import (
    embed_from_postgres,
    delete_file_embeddings,
    collection_stats,
)

from app.retriever import retrieve, build_context
from app.llm import answer


app = FastAPI(
    title="RAG Chatbot API Authenticated",
    version="3.1.0"
)

app.include_router(auth_router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

os.makedirs(DATA_RAW_DIR, exist_ok=True)


@app.on_event("startup")
def startup():
    init_auth_tables()
    init_postgres()


@app.get("/")
def root():
    return {
        "message": "Authenticated RAG Chatbot API running 🚀",
        "version": "3.1.0"
    }


@app.get("/health")
def health():
    status = {
        "api": "ok",
        "postgres": "unknown",
        "chromadb": "unknown"
    }

    try:
        with psycopg2.connect(PG_DSN) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        status["postgres"] = "ok"
    except Exception as e:
        status["postgres"] = f"error: {e}"

    try:
        s = collection_stats()
        status["chromadb"] = "ok"
        status["total_vectors"] = s.get("total_vectors", 0)
    except Exception as e:
        status["chromadb"] = f"error: {e}"

    return status


@app.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    current_user: dict = Depends(get_current_user)
):
    try:
        content = await file.read()
        file_size = len(content)
        file_hash = hashlib.md5(content).hexdigest()

        if file_size == 0:
            raise HTTPException(status_code=400, detail="Uploaded file is empty")

        file_type = classify_file(file.filename)

        if file_type == "unknown":
            raise HTTPException(
                status_code=400,
                detail="Unsupported file type. Supported: CSV, Excel, JSON, PDF, DOCX, TXT, MD, PPTX"
            )

        file.file.seek(0)

        s3_bucket = os.getenv("S3_BUCKET")
        if not s3_bucket:
            raise HTTPException(status_code=500, detail="S3_BUCKET not set")

        # Duplicate check per user
        with psycopg2.connect(PG_DSN) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id
                    FROM app_documents
                    WHERE user_id = %s
                      AND file_hash = %s
                    LIMIT 1
                """, (
                    current_user["id"],
                    file_hash
                ))

                if cur.fetchone():
                    raise HTTPException(
                        status_code=400,
                        detail="Duplicate file already uploaded by this user"
                    )

        # Upload to user-specific S3 prefix
        s3_prefix = f"uploads/user_{current_user['id']}/{file_type}/"

        s3_result = upload_fileobj_to_s3(
            file_obj=file.file,
            filename=file.filename,
            prefix=s3_prefix
        )

        s3_key = s3_result["s3_key"]
        original_filename = s3_result["original_filename"]

        # Re-check after filename sanitization
        file_type = classify_file(original_filename)

        if file_type == "unknown":
            raise HTTPException(status_code=400, detail="Unsupported file type after upload")

        # Store metadata for BOTH structured and unstructured files
        with psycopg2.connect(PG_DSN) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO app_documents
                    (
                        user_id,
                        file_name,
                        file_hash,
                        file_type,
                        s3_key,
                        file_size
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    current_user["id"],
                    original_filename,
                    file_hash,
                    file_type,
                    s3_key,
                    file_size
                ))

                document_id = cur.fetchone()[0]

            conn.commit()

        # STRUCTURED FILE PIPELINE
        # CSV / XLSX / XLS / JSON
        if file_type == "structured":
            return {
                "status": "success",
                "file_type": "structured",
                "message": (
                    "Structured file uploaded to S3. "
                    "Lambda/Glue Crawler will process it. "
                    "Next phase: Glue Job → RDS → dbt → SQL analytics."
                ),
                "file": original_filename,
                "file_size": file_size,
                "chunks": 0,
                "embedded": 0,
                "dbt_status": "Skipped for structured file"
            }

        # UNSTRUCTURED FILE PIPELINE
        # PDF / DOCX / TXT / MD / PPTX
        chunks = ingest_file_from_s3_key(
            bucket=s3_bucket,
            s3_key=s3_key,
            file_name=original_filename,
            file_hash=file_hash,
            user_id=current_user["id"],
            document_id=document_id
        )

        dbt_status = _run_dbt()

        embedded_count = embed_from_postgres(
            user_id=current_user["id"],
            file_name=original_filename
        )

        return {
            "status": "success",
            "file_type": "unstructured",
            "message": "Unstructured file processed and embedded successfully.",
            "file": original_filename,
            "file_size": file_size,
            "chunks": len(chunks),
            "embedded": embedded_count,
            "dbt_status": dbt_status,
        }

    except HTTPException:
        raise

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class ChatRequest(BaseModel):
    question: str
    file_name: Optional[str] = None
    top_k: Optional[int] = 5
    chat_history: Optional[list] = []


@app.post("/chat")
def chat(
    req: ChatRequest,
    current_user: dict = Depends(get_current_user)
):
    if not req.question.strip():
        raise HTTPException(status_code=400, detail="Question cannot be empty")

    # Selected structured file: no Chroma embeddings yet
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
            return {
                "answer": (
                    "This is a structured file. It has been routed to the Glue/RDS/dbt pipeline. "
                    "Structured question answering will be handled by the SQL/NL-to-SQL engine in the next phase."
                ),
                "chunks": [],
                "sources": [req.file_name],
                "chunks_used": 0,
                "file_type": "structured"
            }

    chunks = retrieve(
        req.question,
        user_id=current_user["id"],
        top_k=req.top_k,
        file_name=req.file_name
    )

    if not chunks:
        return {
            "answer": "No relevant documents found for your account. Please upload an unstructured file first.",
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
                    file_name
                )
                VALUES (%s, %s, %s, %s)
            """, (
                current_user["id"],
                req.question,
                response,
                req.file_name
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


@app.get("/files")
def list_files(current_user: dict = Depends(get_current_user)):
    """
    Return all files uploaded by the logged-in user.
    Includes both structured and unstructured files.
    """
    try:
        with psycopg2.connect(PG_DSN) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        d.file_name,
                        COALESCE(d.file_type, 'unknown') AS file_type,
                        d.file_size,
                        d.uploaded_at,
                        COALESCE(COUNT(r.chunk_id), 0) AS chunk_count
                    FROM app_documents d
                    LEFT JOIN raw_chunks r
                        ON r.document_id = d.id
                       AND r.user_id = d.user_id
                    WHERE d.user_id = %s
                    GROUP BY
                        d.id,
                        d.file_name,
                        d.file_type,
                        d.file_size,
                        d.uploaded_at
                    ORDER BY d.uploaded_at DESC
                """, (
                    current_user["id"],
                ))

                rows = cur.fetchall()

        return {
            "files": [
                {
                    "name": r[0],
                    "file_type": r[1],
                    "size": r[2] or 0,
                    "uploaded_at": r[3].isoformat() if r[3] else None,
                    "chunks": r[4] or 0,
                }
                for r in rows
            ]
        }

    except Exception as e:
        return {
            "files": [],
            "error": str(e)
        }


@app.delete("/files/{file_name}")
def delete_file(
    file_name: str,
    current_user: dict = Depends(get_current_user)
):
    # get document info first
    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, file_type, s3_key
                FROM app_documents
                WHERE user_id = %s
                  AND file_name = %s
                LIMIT 1
            """, (
                current_user["id"],
                file_name
            ))

            doc = cur.fetchone()

    if not doc:
        raise HTTPException(status_code=404, detail="File not found")

    document_id, file_type, s3_key = doc

    # delete raw chunks and embeddings for unstructured file
    delete_file_chunks(
        current_user["id"],
        file_name
    )

    delete_file_embeddings(
        current_user["id"],
        file_name
    )

    # delete from S3
    if s3_key:
        delete_s3_object(s3_key)

    # delete metadata
    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM app_documents
                WHERE user_id = %s
                  AND id = %s
            """, (
                current_user["id"],
                document_id
            ))

        conn.commit()

    return {
        "status": "deleted",
        "user_id": current_user["id"],
        "file": file_name,
        "file_type": file_type,
        "s3_deleted": bool(s3_key)
    }

@app.get("/history")
def get_history(current_user: dict = Depends(get_current_user)):
    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    question,
                    answer,
                    file_name,
                    created_at
                FROM chat_history
                WHERE user_id = %s
                ORDER BY created_at DESC
                LIMIT 50
            """, (
                current_user["id"],
            ))

            rows = cur.fetchall()

    return {
        "history": [
            {
                "question": r[0],
                "answer": r[1],
                "file_name": r[2],
                "created_at": r[3].isoformat() if r[3] else None,
            }
            for r in rows
        ]
    }


@app.get("/stats")
def stats(current_user: dict = Depends(get_current_user)):
    base = collection_stats()

    try:
        with psycopg2.connect(PG_DSN) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*)
                    FROM app_documents
                    WHERE user_id = %s
                """, (
                    current_user["id"],
                ))
                base["pg_files"] = cur.fetchone()[0]

                cur.execute("""
                    SELECT COUNT(*)
                    FROM app_documents
                    WHERE user_id = %s
                      AND file_type = 'structured'
                """, (
                    current_user["id"],
                ))
                base["structured_files"] = cur.fetchone()[0]

                cur.execute("""
                    SELECT COUNT(*)
                    FROM app_documents
                    WHERE user_id = %s
                      AND file_type = 'unstructured'
                """, (
                    current_user["id"],
                ))
                base["unstructured_files"] = cur.fetchone()[0]

                cur.execute("""
                    SELECT COUNT(*)
                    FROM raw_chunks
                    WHERE user_id = %s
                """, (
                    current_user["id"],
                ))
                base["pg_raw_chunks"] = cur.fetchone()[0]

                try:
                    cur.execute("""
                        SELECT COUNT(*)
                        FROM mart_processed_chunks
                        WHERE user_id = %s
                    """, (
                        current_user["id"],
                    ))
                    base["pg_mart_chunks"] = cur.fetchone()[0]
                except Exception:
                    conn.rollback()

    except Exception as e:
        base["error"] = str(e)

    return base


@app.post("/dbt/run")
def run_dbt(current_user: dict = Depends(get_current_user)):
    return {
        "user_id": current_user["id"],
        "dbt_status": _run_dbt()
    }


def _run_dbt():
    try:
        result = subprocess.run(
            ["dbt", "build"],
            cwd=DBT_PROJECT_DIR,
            capture_output=True,
            text=True,
            timeout=120
        )

        output = (result.stdout or "") + (result.stderr or "")

        if result.returncode == 0:
            return "dbt build succeeded"

        return f"dbt failed:\n{output[-800:]}"

    except Exception as e:
        return f"dbt error: {str(e)}"
