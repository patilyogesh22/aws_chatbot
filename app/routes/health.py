import psycopg2
from fastapi import APIRouter

from app.config import PG_DSN
from app.services.embedding_service import collection_stats

router = APIRouter()


@router.get("/health")
def health():
    status = {
        "api": "ok",
        "postgres": "unknown",
        "pgvector": "unknown"
    }

    try:
        with psycopg2.connect(PG_DSN) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")

        status["postgres"] = "ok"

    except Exception as e:
        status["postgres"] = f"error: {e}"

    try:
        stats = collection_stats()
        status["pgvector"] = "ok"
        status["total_vectors"] = stats.get("total_vectors", 0)

    except Exception as e:
        status["pgvector"] = f"error: {e}"

    return status