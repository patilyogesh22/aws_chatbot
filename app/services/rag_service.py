"""
retriever.py
Semantic search over PostgreSQL pgvector to retrieve top-K relevant chunks
for a given user query.

User isolation:
- Every query must include user_id
- Optional file_name filter is applied inside that user's data only
"""

from typing import List, Dict, Optional

import psycopg2
from pgvector.psycopg2 import register_vector
from sentence_transformers import SentenceTransformer

from app.config import (
    PG_DSN,
    EMBEDDING_MODEL,
    TOP_K,
)

_model: SentenceTransformer = None


def _get_model() -> SentenceTransformer:
    global _model

    if _model is None:
        print(f"[retriever] Loading model: {EMBEDDING_MODEL}")
        _model = SentenceTransformer(EMBEDDING_MODEL)

    return _model


def retrieve(
    query: str,
    user_id: int,
    top_k: int = TOP_K,
    file_name: Optional[str] = None,
) -> List[Dict]:
    """
    Retrieve top_k most relevant chunks for the logged-in user
    from PostgreSQL pgvector.

    Returns:
        [
            {
                "chunk_text": "...",
                "file_name": "...",
                "chunk_id": "...",
                "chunk_index": 0,
                "score": 0.91
            }
        ]
    """

    if not query or not query.strip():
        return []

    model = _get_model()
    query_embedding = model.encode(query).tolist()

    conn = psycopg2.connect(PG_DSN)

    try:
        register_vector(conn)

        with conn.cursor() as cur:
            if file_name:
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
                    file_name,
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

        chunks = []

        for r in rows:
            chunks.append({
                "chunk_text": r[0],
                "file_name": r[1],
                "chunk_id": r[2],
                "chunk_index": r[3],
                "score": round(float(r[4]), 4) if r[4] is not None else 0.0,
            })

        return chunks

    finally:
        conn.close()


def build_context(chunks: List[Dict]) -> str:
    """
    Concatenate retrieved chunks into a single context string for the LLM.
    """

    parts = []

    for i, c in enumerate(chunks, 1):
        parts.append(
            f"[Chunk {i} | File: {c['file_name']} | Score: {c['score']}]\n"
            f"{c['chunk_text']}"
        )

    return "\n\n---\n\n".join(parts)