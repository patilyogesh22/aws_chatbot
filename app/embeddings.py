"""
embeddings.py
Reads processed chunks from PostgreSQL, generates embeddings,
and stores them in PostgreSQL pgvector with user-level isolation.

This replaces ChromaDB so local and EC2 can use the same RDS vector table.
"""

import json
from typing import List, Dict, Optional

import psycopg2
from pgvector.psycopg2 import register_vector
from sentence_transformers import SentenceTransformer

from app.config import (
    PG_DSN,
    EMBEDDING_MODEL,
)

_model: SentenceTransformer = None


def _get_model() -> SentenceTransformer:
    global _model

    if _model is None:
        print(f"[embeddings] Loading model: {EMBEDDING_MODEL}")
        _model = SentenceTransformer(EMBEDDING_MODEL)

    return _model


def init_pgvector():
    """
    Create pgvector extension and embeddings table.
    """

    conn = psycopg2.connect(PG_DSN)

    try:
        register_vector(conn)

        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")

            cur.execute("""
                CREATE TABLE IF NOT EXISTS document_embeddings (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    document_id INTEGER,
                    file_name TEXT NOT NULL,
                    file_hash TEXT,
                    chunk_id TEXT NOT NULL,
                    chunk_index INTEGER,
                    word_count INTEGER,
                    chunk_text TEXT NOT NULL,
                    embedding vector(384),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS ux_document_embeddings_chunk
                ON document_embeddings(user_id, file_name, chunk_id);
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_document_embeddings_user_file
                ON document_embeddings(user_id, file_name);
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_document_embeddings_vector
                ON document_embeddings
                USING ivfflat (embedding vector_cosine_ops)
                WITH (lists = 100);
            """)

        conn.commit()
        print("[embeddings] pgvector initialized")

    finally:
        conn.close()


def _fetch_chunks_from_pg(
    user_id: int,
    file_name: Optional[str] = None,
) -> List[Dict]:
    """
    Read processed chunks from PostgreSQL.

    First tries mart_processed_chunks.
    If dbt mart is not available, falls back to raw_chunks.
    """

    conn = psycopg2.connect(PG_DSN)

    try:
        with conn.cursor() as cur:
            for table in ("mart_processed_chunks", "raw_chunks"):
                try:
                    if file_name:
                        cur.execute(f"""
                            SELECT
                                chunk_id,
                                chunk_text,
                                file_name,
                                chunk_index,
                                word_count,
                                user_id,
                                document_id,
                                file_hash
                            FROM {table}
                            WHERE user_id = %s
                              AND file_name = %s
                            ORDER BY chunk_index
                        """, (user_id, file_name))
                    else:
                        cur.execute(f"""
                            SELECT
                                chunk_id,
                                chunk_text,
                                file_name,
                                chunk_index,
                                word_count,
                                user_id,
                                document_id,
                                file_hash
                            FROM {table}
                            WHERE user_id = %s
                            ORDER BY file_name, chunk_index
                        """, (user_id,))

                    rows = cur.fetchall()

                    print(f"[embeddings] Reading from table: {table}")

                    return [
                        {
                            "chunk_id": r[0],
                            "chunk_text": r[1],
                            "file_name": r[2],
                            "chunk_index": r[3],
                            "word_count": r[4],
                            "user_id": r[5],
                            "document_id": r[6],
                            "file_hash": r[7],
                        }
                        for r in rows
                    ]

                except psycopg2.errors.UndefinedTable:
                    conn.rollback()
                    continue

                except psycopg2.errors.UndefinedColumn:
                    conn.rollback()
                    continue

        return []

    finally:
        conn.close()


def embed_and_store(chunks: List[Dict]) -> int:
    """
    Takes chunk dicts and stores embeddings in PostgreSQL pgvector.
    """

    if not chunks:
        return 0

    init_pgvector()

    model = _get_model()

    texts = [c["chunk_text"] for c in chunks]

    print(f"[embeddings] Generating embeddings for {len(texts)} chunks...")
    embeddings = model.encode(texts, show_progress_bar=True).tolist()

    conn = psycopg2.connect(PG_DSN)

    try:
        register_vector(conn)

        inserted = 0

        with conn.cursor() as cur:
            for c, emb in zip(chunks, embeddings):
                cur.execute("""
                    INSERT INTO document_embeddings
                    (
                        user_id,
                        document_id,
                        file_name,
                        file_hash,
                        chunk_id,
                        chunk_index,
                        word_count,
                        chunk_text,
                        embedding
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (user_id, file_name, chunk_id)
                    DO UPDATE SET
                        document_id = EXCLUDED.document_id,
                        file_hash = EXCLUDED.file_hash,
                        chunk_index = EXCLUDED.chunk_index,
                        word_count = EXCLUDED.word_count,
                        chunk_text = EXCLUDED.chunk_text,
                        embedding = EXCLUDED.embedding,
                        created_at = CURRENT_TIMESTAMP
                """, (
                    int(c.get("user_id")),
                    c.get("document_id"),
                    c.get("file_name"),
                    c.get("file_hash"),
                    c.get("chunk_id"),
                    c.get("chunk_index"),
                    c.get("word_count"),
                    c.get("chunk_text"),
                    emb,
                ))

                inserted += 1

        conn.commit()

        print(f"[embeddings] Stored {inserted} vectors in PostgreSQL pgvector")
        return inserted

    finally:
        conn.close()


def embed_from_postgres(
    user_id: int,
    file_name: Optional[str] = None,
) -> int:
    """
    Read logged-in user's chunks from PostgreSQL and embed them.
    """

    chunks = _fetch_chunks_from_pg(
        user_id=user_id,
        file_name=file_name,
    )

    return embed_and_store(chunks)


def delete_file_embeddings(user_id: int, file_name: str):
    """
    Delete embeddings only for logged-in user's selected file.
    """

    init_pgvector()

    conn = psycopg2.connect(PG_DSN)

    try:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM document_embeddings
                WHERE user_id = %s
                  AND file_name = %s
            """, (
                user_id,
                file_name,
            ))

            deleted = cur.rowcount

        conn.commit()

        print(
            f"[embeddings] Deleted {deleted} vectors "
            f"for user={user_id}, file='{file_name}'"
        )

    finally:
        conn.close()


def collection_stats(user_id: int = None) -> Dict:
    init_pgvector()

    conn = psycopg2.connect(PG_DSN)

    try:
        with conn.cursor() as cur:
            if user_id:
                cur.execute("""
                    SELECT COUNT(*)
                    FROM document_embeddings
                    WHERE user_id = %s
                """, (user_id,))
            else:
                cur.execute("""
                    SELECT COUNT(*)
                    FROM document_embeddings
                """)

            total_vectors = cur.fetchone()[0]

        return {
            "total_vectors": total_vectors
        }

    finally:
        conn.close()