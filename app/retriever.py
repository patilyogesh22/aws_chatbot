"""
retriever.py
Semantic search over ChromaDB to retrieve top-K relevant chunks
for a given user query.

User isolation:
- Every query must include user_id
- Optional file_name filter is applied inside that user's data only
"""

from typing import List, Dict, Optional

import os
import chromadb
from chromadb.config import Settings
from sentence_transformers import SentenceTransformer

from app.config import (
    VECTOR_STORE_DIR,
    CHROMA_COLLECTION,
    EMBEDDING_MODEL,
    TOP_K,
)

_model: SentenceTransformer = None
_collection = None


def _get_model():
    global _model
    if _model is None:
        _model = SentenceTransformer(EMBEDDING_MODEL)
    return _model


def _get_collection():
    global _collection
    if _collection is None:
        os.makedirs(VECTOR_STORE_DIR, exist_ok=True)

        client = chromadb.PersistentClient(
            path=VECTOR_STORE_DIR,
            settings=Settings(anonymized_telemetry=False),
        )

        _collection = client.get_or_create_collection(
            name=CHROMA_COLLECTION,
            metadata={"hnsw:space": "cosine"},
        )

    return _collection


def retrieve(
    query: str,
    user_id: int,
    top_k: int = TOP_K,
    file_name: Optional[str] = None,
) -> List[Dict]:
    """
    Retrieve top_k most relevant chunks for the logged-in user.

    Args:
        query: User's question.
        user_id: Logged-in user's id.
        top_k: Number of chunks to return.
        file_name: Optional file filter. If provided, search only this file
                   inside the logged-in user's documents.

    Returns:
        List of dicts:
        {
            chunk_text,
            file_name,
            chunk_index,
            score
        }
    """

    if not query or not query.strip():
        return []

    model = _get_model()
    collection = _get_collection()

    total_vectors = collection.count()
    if total_vectors == 0:
        return []

    query_embedding = model.encode([query]).tolist()

    # IMPORTANT:
    # ChromaDB metadata values are stored as strings in embeddings.py,
    # so user_id must also be compared as string.
    if file_name:
        where_filter = {
            "$and": [
                {"user_id": str(user_id)},
                {"file_name": file_name},
            ]
        }
    else:
        where_filter = {
            "user_id": str(user_id)
        }

    results = collection.query(
        query_embeddings=query_embedding,
        n_results=min(top_k, total_vectors),
        where=where_filter,
        include=["documents", "metadatas", "distances"],
    )

    documents = results.get("documents", [[]])[0]
    metadatas = results.get("metadatas", [[]])[0]
    distances = results.get("distances", [[]])[0]

    chunks = []

    for doc, meta, dist in zip(documents, metadatas, distances):
        chunks.append({
            "chunk_text": doc,
            "file_name": meta.get("file_name", ""),
            "chunk_index": int(meta.get("chunk_index", 0)),
            "score": round(1 - dist, 4),
        })

    return chunks


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