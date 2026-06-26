import psycopg2

from fastapi import APIRouter, Depends

from app.config import PG_DSN
from app.auth import get_current_user
from app.services.embedding_service import collection_stats

router = APIRouter()


@router.get("/stats")
def stats(current_user: dict = Depends(get_current_user)):
    base = collection_stats(user_id=current_user["id"])

    try:
        with psycopg2.connect(PG_DSN) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*)
                    FROM app_documents
                    WHERE user_id = %s
                """, (current_user["id"],))
                base["pg_files"] = cur.fetchone()[0]

                cur.execute("""
                    SELECT COUNT(*)
                    FROM app_documents
                    WHERE user_id = %s
                      AND file_type = 'structured'
                """, (current_user["id"],))
                base["structured_files"] = cur.fetchone()[0]

                cur.execute("""
                    SELECT COUNT(*)
                    FROM app_documents
                    WHERE user_id = %s
                      AND file_type = 'unstructured'
                """, (current_user["id"],))
                base["unstructured_files"] = cur.fetchone()[0]

                cur.execute("""
                    SELECT COUNT(*)
                    FROM raw_chunks
                    WHERE user_id = %s
                """, (current_user["id"],))
                base["pg_raw_chunks"] = cur.fetchone()[0]

                try:
                    cur.execute("""
                        SELECT COUNT(*)
                        FROM mart_processed_chunks
                        WHERE user_id = %s
                    """, (current_user["id"],))
                    base["pg_mart_chunks"] = cur.fetchone()[0]
                except Exception:
                    conn.rollback()
                    base["pg_mart_chunks"] = 0

    except Exception as e:
        base["error"] = str(e)

    return base