"""
embeddings.py
Reads processed chunks from PostgreSQL (dbt mart output),
generates embeddings, and upserts into ChromaDB.
"""
import os
from typing import List, Dict
import psycopg2
import chromadb
from chromadb.config import Settings
from sentence_transformers import SentenceTransformer
from app.config import (
    PG_DSN, VECTOR_STORE_DIR, CHROMA_COLLECTION, EMBEDDING_MODEL
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
            settings=Settings(anonymized_telemetry=False)
        )
        _collection = _chroma_client.get_or_create_collection(
            name=CHROMA_COLLECTION,
            metadata={"hnsw:space": "cosine"}
        )
    return _collection


def embed_and_store(chunks: List[Dict]) -> int:
    """
    Takes a list of chunk dicts and stores embeddings in ChromaDB.
    Returns number of chunks embedded.
    """
    if not chunks:
        return 0

    model      = _get_model()
    collection = _get_collection()

    texts     = [c["chunk_text"] for c in chunks]
    ids       = [c["chunk_id"]   for c in chunks]
    metadatas = [
        {
            "file_name":   c.get("file_name", ""),
            "chunk_index": str(c.get("chunk_index", 0)),
            "word_count":  str(c.get("word_count", 0)),
        }
        for c in chunks
    ]

    print(f"[embeddings] Generating embeddings for {len(texts)} chunks…")
    embeddings = model.encode(texts, show_progress_bar=True).tolist()

    collection.upsert(
        ids=ids,
        embeddings=embeddings,
        documents=texts,
        metadatas=metadatas,
    )
    print(f"[embeddings] Stored {len(texts)} vectors in ChromaDB")
    return len(texts)


def _fetch_chunks_from_pg(file_name: str = None) -> List[Dict]:
    """
    Read processed chunks from PostgreSQL.
    Tries mart_processed_chunks first (dbt output),
    falls back to raw_chunks if mart table doesn't exist yet.
    """
    conn = psycopg2.connect(PG_DSN)
    try:
        with conn.cursor() as cur:
            # Try dbt mart table first
            for table in ("mart_processed_chunks", "raw_chunks"):
                try:
                    if file_name:
                        cur.execute(
                            f"SELECT chunk_id, chunk_text, file_name, "
                            f"chunk_index, word_count FROM {table} "
                            f"WHERE file_name = %s",
                            (file_name,)
                        )
                    else:
                        cur.execute(
                            f"SELECT chunk_id, chunk_text, file_name, "
                            f"chunk_index, word_count FROM {table}"
                        )
                    rows = cur.fetchall()
                    print(f"[embeddings] Reading from table: {table}")
                    return [
                        {"chunk_id": r[0], "chunk_text": r[1],
                         "file_name": r[2], "chunk_index": r[3],
                         "word_count": r[4]}
                        for r in rows
                    ]
                except psycopg2.errors.UndefinedTable:
                    conn.rollback()
                    continue
        return []
    finally:
        conn.close()


def embed_from_postgres(file_name: str = None) -> int:
    """Read processed chunks from PostgreSQL and embed them."""
    chunks = _fetch_chunks_from_pg(file_name)
    return embed_and_store(chunks)


def delete_file_embeddings(file_name: str):
    """Remove all embeddings for a given file from ChromaDB."""
    collection = _get_collection()
    results = collection.get(where={"file_name": file_name})
    if results["ids"]:
        collection.delete(ids=results["ids"])
        print(f"[embeddings] Deleted {len(results['ids'])} vectors for '{file_name}'")


def collection_stats() -> Dict:
    collection = _get_collection()
    return {"total_vectors": collection.count()}