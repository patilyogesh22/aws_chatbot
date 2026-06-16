"""
main.py — FastAPI backend (PostgreSQL + S3 version)

Pipeline:
Upload → S3 → S3 ingestion → PostgreSQL → dbt → embeddings
"""

import os
import hashlib
import subprocess
from typing import Optional

from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from app.config import DATA_RAW_DIR, DBT_PROJECT_DIR, PG_DSN

from aws.s3_ingestion import upload_fileobj_to_s3
from app.ingestion import ingest_file_from_s3_key,delete_file_chunks, list_ingested_files_meta, init_postgres

from app.embeddings import (
    embed_from_postgres,
    delete_file_embeddings,
    collection_stats,
)

from app.retriever import retrieve, build_context
from app.llm import answer


# ─────────────────────────────────────────────
# APP INIT
# ─────────────────────────────────────────────

app = FastAPI(title="RAG Chatbot API (S3 + PostgreSQL)", version="2.0.0")
@app.on_event("startup")
def startup():
    init_postgres()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

os.makedirs(DATA_RAW_DIR, exist_ok=True)


# ─────────────────────────────────────────────
# HEALTH CHECK
# ─────────────────────────────────────────────

@app.get("/health")
def health():
    status = {"api": "ok", "postgres": "unknown", "chromadb": "unknown"}

    try:
        import psycopg2
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


# ─────────────────────────────────────────────
# /UPLOAD (S3 + RDS + DBT + EMBEDDINGS)
# ─────────────────────────────────────────────

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):

    try:
        # ── Read file
        content = await file.read()
        file_size = len(content)

        # ── Generate file identity hash (IMPORTANT FIX)
        file_hash = hashlib.md5(content).hexdigest()

        file.file.seek(0)

        # ─────────────────────────────
        # STEP 0: DUPLICATE CHECK (NEW)
        # ─────────────────────────────
        import psycopg2
        with psycopg2.connect(PG_DSN) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 1 FROM raw_chunks WHERE file_hash = %s LIMIT 1
                """, (file_hash,))
                if cur.fetchone():
                    raise HTTPException(
                        status_code=400,
                        detail="Duplicate file already uploaded"
                    )

        # ─────────────────────────────
        # STEP 1: Upload to S3
        # ─────────────────────────────
        s3_bucket = os.getenv("S3_BUCKET")

        if not s3_bucket:
            raise HTTPException(status_code=500, detail="S3_BUCKET not set")

        s3_result = upload_fileobj_to_s3(
            file_obj=file.file,
            filename=file.filename,
            prefix="uploads/"
        )

        s3_key = s3_result["s3_key"]
        original_filename = s3_result["original_filename"]

        # ─────────────────────────────
        # STEP 2: Ingest from S3 → PostgreSQL
        # ─────────────────────────────
        chunks = ingest_file_from_s3_key(
            bucket=s3_bucket,
            s3_key=s3_key,
            file_name=original_filename,
            file_hash=file_hash   # ✅ ADD THIS
        )

        # ─────────────────────────────
        # STEP 3: Run dbt
        # ─────────────────────────────
        dbt_status = _run_dbt()

        # ─────────────────────────────
        # STEP 4: Embeddings
        # ─────────────────────────────
        n = embed_from_postgres(file_name=original_filename)

        return {
            "status": "success",
            "file": original_filename,   # FIXED (clean name always)
            "s3_key": s3_key,
            "file_size": file_size,
            "chunks": len(chunks),
            "embedded": n,
            "dbt_status": dbt_status,
            "file_hash": file_hash       # useful debug info
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
# ─────────────────────────────────────────────
# CHAT API
# ─────────────────────────────────────────────

class ChatRequest(BaseModel):
    question: str
    file_name: Optional[str] = None
    top_k: Optional[int] = 5
    chat_history: Optional[list] = []


@app.post("/chat")
def chat(req: ChatRequest):

    if not req.question.strip():
        raise HTTPException(status_code=400, detail="Question cannot be empty")

    chunks = retrieve(req.question, top_k=req.top_k, file_name=req.file_name)

    if not chunks:
        return {
            "answer": "No relevant documents found. Please upload a file first.",
            "chunks": [],
            "sources": [],
            "chunks_used": 0,
        }

    context = build_context(chunks)
    response = answer(req.question, context, req.chat_history)

    sources = list({c["file_name"] for c in chunks})

    return {
        "answer": response,
        "chunks": chunks,
        "sources": sources,
        "chunks_used": len(chunks),
        "model": os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile"),
    }


# ─────────────────────────────────────────────
# FILES
# ─────────────────────────────────────────────

@app.get("/files")
def list_files():
    try:
        return {"files": list_ingested_files_meta()}
    except Exception:
        return {"files": []}


@app.delete("/files/{file_name}")
def delete_file(file_name: str):

    delete_file_chunks(file_name)
    delete_file_embeddings(file_name)

    return {"status": "deleted", "file": file_name}


# ─────────────────────────────────────────────
# STATS
# ─────────────────────────────────────────────

@app.get("/stats")
def stats():
    base = collection_stats()

    try:
        import psycopg2
        with psycopg2.connect(PG_DSN) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM raw_chunks")
                base["pg_raw_chunks"] = cur.fetchone()[0]

                cur.execute("SELECT COUNT(DISTINCT file_name) FROM raw_chunks")
                base["pg_files"] = cur.fetchone()[0]

                try:
                    cur.execute("SELECT COUNT(*) FROM mart_processed_chunks")
                    base["pg_mart_chunks"] = cur.fetchone()[0]
                except:
                    conn.rollback()

    except:
        pass

    return base


# ─────────────────────────────────────────────
# DBT RUN
# ─────────────────────────────────────────────

@app.post("/dbt/run")
def run_dbt():
    return {"dbt_status": _run_dbt()}


def _run_dbt():
    try:
        result = subprocess.run(
            ["dbt", "build", "--profiles-dir", "/root/.dbt"],
            cwd=DBT_PROJECT_DIR,
            capture_output=True,
            text=True,
            timeout=120
        )

        output = (result.stdout or "") + (result.stderr or "")

        if result.returncode == 0:
            return "dbt build succeeded"
        else:
            return f"dbt failed:\n{output[-800:]}"

    except Exception as e:
        return f"dbt error: {str(e)}"


# ─────────────────────────────────────────────
# ROOT
# ─────────────────────────────────────────────

@app.get("/")
def root():
    return {"message": "RAG Chatbot API running 🚀"}


