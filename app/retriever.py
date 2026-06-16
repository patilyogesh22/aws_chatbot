"""
retriever.py
Semantic search over ChromaDB to retrieve top-K relevant chunks
for a given user query.
"""
from typing import List, Dict
from sentence_transformers import SentenceTransformer
import chromadb
from chromadb.config import Settings
import os
from app.config import VECTOR_STORE_DIR, CHROMA_COLLECTION, EMBEDDING_MODEL, TOP_K

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
            settings=Settings(anonymized_telemetry=False)
        )
        _collection = client.get_or_create_collection(
            name=CHROMA_COLLECTION,
            metadata={"hnsw:space": "cosine"}
        )
    return _collection


def retrieve(query: str, top_k: int = TOP_K,
             file_name: str = None) -> List[Dict]:
    """
    Retrieve top_k most relevant chunks for the query.

    Args:
        query:     User's question.
        top_k:     Number of chunks to return.
        file_name: Optional filter — only search within a specific file.

    Returns:
        List of dicts: {chunk_text, file_name, chunk_index, score}
    """
    model      = _get_model()
    collection = _get_collection()

    if collection.count() == 0:
        return []

    query_embedding = model.encode([query]).tolist()

    where = {"file_name": file_name} if file_name else None

    results = collection.query(
        query_embeddings=query_embedding,
        n_results=min(top_k, collection.count()),
        where=where,
        include=["documents", "metadatas", "distances"],
    )

    chunks = []
    for doc, meta, dist in zip(
        results["documents"][0],
        results["metadatas"][0],
        results["distances"][0],
    ):
        chunks.append({
            "chunk_text":  doc,
            "file_name":   meta.get("file_name", ""),
            "chunk_index": int(meta.get("chunk_index", 0)),
            "score":       round(1 - dist, 4),   # cosine similarity
        })

    return chunks


def build_context(chunks: List[Dict]) -> str:
    """Concatenate retrieved chunks into a single context string for the LLM."""
    parts = []
    for i, c in enumerate(chunks, 1):
        parts.append(
            f"[Chunk {i} | File: {c['file_name']} | Score: {c['score']}]\n"
            f"{c['chunk_text']}"
        )
    return "\n\n---\n\n".join(parts)