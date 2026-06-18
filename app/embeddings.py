"""
embeddings.py
Reads processed chunks from PostgreSQL, generates embeddings,
and stores them in ChromaDB with user-level isolation.
"""

import os
from typing import List, Dict, Optional

import psycopg2
import chromadb
from chromadb.config import Settings
from sentence_transformers import SentenceTransformer

from app.config import (
    PG_DSN,
    VECTOR_STORE_DIR,
    CHROMA_COLLECTION,
    EMBEDDING_MODEL,
)

_model: SentenceTransformer = None
_chroma_client = None
_collection = None


def _get_model() -> SentenceTransformer:
    global _model

    if _model is None:
        print(f"[embeddings] Loading model: {EMBEDDING_MODEL}")
        _model = SentenceTransformer(EMBEDDING_MODEL)

    return _model


def _get_collection():
    global _chroma_client, _collection

    if _collection is None:
        os.makedirs(VECTOR_STORE_DIR, exist_ok=True)

        _chroma_client = chromadb.PersistentClient(
            path=VECTOR_STORE_DIR,
            settings=Settings(anonymized_telemetry=False),
        )

        _collection = _chroma_client.get_or_create_collection(
            name=CHROMA_COLLECTION,
            metadata={"hnsw:space": "cosine"},
        )

    return _collection


def embed_and_store(chunks: List[Dict]) -> int:
    """
    Takes chunk dicts and stores embeddings in ChromaDB.

    Every vector stores:
    - user_id
    - document_id
    - file_name
    - file_hash
    - chunk_index

    This is required so retrieval can filter by logged-in user.
    """

    if not chunks:
        return 0

    model = _get_model()
    collection = _get_collection()

    texts = [c["chunk_text"] for c in chunks]
    ids = [c["chunk_id"] for c in chunks]

    metadatas = [
        {
            "user_id": str(c.get("user_id", "")),
            "document_id": str(c.get("document_id", "")),
            "file_name": c.get("file_name", ""),
            "file_hash": c.get("file_hash", ""),
            "chunk_index": str(c.get("chunk_index", 0)),
            "word_count": str(c.get("word_count", 0)),
        }
        for c in chunks
    ]

    print(f"[embeddings] Generating embeddings for {len(texts)} chunks...")
    embeddings = model.encode(texts, show_progress_bar=True).tolist()

    collection.upsert(
        ids=ids,
        embeddings=embeddings,
        documents=texts,
        metadatas=metadatas,
    )

    print(f"[embeddings] Stored {len(texts)} vectors in ChromaDB")
    return len(texts)


def _fetch_chunks_from_pg(
    user_id: int,
    file_name: Optional[str] = None,
) -> List[Dict]:
    """
    Read processed chunks from PostgreSQL.

    First tries mart_processed_chunks.
    If dbt mart is not available, falls back to raw_chunks.

    Important:
    Both tables must include user_id.
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

    collection = _get_collection()

    results = collection.get(
        where={
            "$and": [
                {"user_id": str(user_id)},
                {"file_name": file_name},
            ]
        }
    )

    ids = results.get("ids", [])

    if ids:
        collection.delete(ids=ids)
        print(
            f"[embeddings] Deleted {len(ids)} vectors "
            f"for user={user_id}, file='{file_name}'"
        )


def collection_stats() -> Dict:
    """
    Global Chroma stats.
    """

    collection = _get_collection()

    return {
        "total_vectors": collection.count()
    }