"""
rag_service.py
Semantic search over PostgreSQL pgvector to retrieve top-K relevant chunks.

Supports:
- one selected file with file_name
- multiple selected files with file_names
- all user files when no file filter is passed
"""

from typing import List, Dict, Optional

import psycopg2
from pgvector.psycopg2 import register_vector
from sentence_transformers import SentenceTransformer
from functools import lru_cache 
from app.config import (
    PG_DSN,
    EMBEDDING_MODEL,
    TOP_K,
)
from app.db import get_db_connection

_model: SentenceTransformer = None


def _get_model() -> SentenceTransformer:
    global _model

    if _model is None:
        print(f"[retriever] Loading model: {EMBEDDING_MODEL}")
        _model = SentenceTransformer(EMBEDDING_MODEL)

    return _model

@lru_cache(maxsize=256)
def _cached_embed(text: str):
    """
    Cache query embeddings in memory.
    Same question = no repeated embedding calculation.
    """
    model = _get_model()
    normalized = text.strip().lower()
    return tuple(model.encode(normalized).tolist())

def retrieve(
    query: str,
    user_id: int,
    top_k: int = TOP_K,
    file_name: Optional[str] = None,
    file_names: Optional[list[str]] = None,
) -> List[Dict]:
    """
    Retrieve top_k most relevant chunks for the logged-in user.

    Priority:
    1. file_names with multiple values -> search selected files
    2. file_name or single item file_names -> search one file
    3. no filter -> search all files for the user
    """

    if not query or not query.strip():
        return []

    query_embedding = list(_cached_embed(query))

    # Normalize file filters
    clean_file_names = []
    if file_names:
        seen = set()
        for f in file_names:
            if f and f not in seen:
                clean_file_names.append(f)
                seen.add(f)

    if file_name and not clean_file_names:
        clean_file_names = [file_name]

    conn_cm = get_db_connection()
    conn = conn_cm.__enter__()

    try:
        register_vector(conn)

        with conn.cursor() as cur:
            if len(clean_file_names) > 1:
                cur.execute("""
                    SELECT
                        chunk_text,
                        file_name,
                        chunk_id,
                        chunk_index,
                        1 - (embedding <=> %s::vector) AS score
                    FROM document_embeddings
                    WHERE user_id = %s
                      AND file_name = ANY(%s)
                    ORDER BY embedding <=> %s::vector
                    LIMIT %s
                """, (
                    query_embedding,
                    user_id,
                    clean_file_names,
                    query_embedding,
                    top_k,
                ))

            elif len(clean_file_names) == 1:
                cur.execute("""
                    SELECT
                        chunk_text,
                        file_name,
                        chunk_id,
                        chunk_index,
                        1 - (embedding <=> %s::vector) AS score
                    FROM document_embeddings
                    WHERE user_id = %s
                      AND file_name = %s
                    ORDER BY embedding <=> %s::vector
                    LIMIT %s
                """, (
                    query_embedding,
                    user_id,
                    clean_file_names[0],
                    query_embedding,
                    top_k,
                ))

            else:
                cur.execute("""
                    SELECT
                        chunk_text,
                        file_name,
                        chunk_id,
                        chunk_index,
                        1 - (embedding <=> %s::vector) AS score
                    FROM document_embeddings
                    WHERE user_id = %s
                    ORDER BY embedding <=> %s::vector
                    LIMIT %s
                """, (
                    query_embedding,
                    user_id,
                    query_embedding,
                    top_k,
                ))

            rows = cur.fetchall()

        return [
            {
                "chunk_text": r[0],
                "file_name": r[1],
                "chunk_id": r[2],
                "chunk_index": r[3],
                "score": round(float(r[4]), 4) if r[4] is not None else 0.0,
            }
            for r in rows
        ]

    finally:
        conn_cm.__exit__(None, None, None)


def build_context(chunks: List[Dict]) -> str:
    parts = []

    for i, c in enumerate(chunks, 1):
        parts.append(
            f"[Chunk {i} | File: {c['file_name']} | Score: {c['score']}]\n"
            f"{c['chunk_text']}"
        )

    return "\n\n---\n\n".join(parts)
