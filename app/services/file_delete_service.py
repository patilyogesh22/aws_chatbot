import re
import psycopg2

from app.config import PG_DSN
from app.db import get_db_connection
from aws.s3_ingestion import delete_s3_object
from app.services.ingestion_service import delete_file_chunks
from app.services.embedding_service import delete_file_embeddings


def safe_drop_table(cur, table_name: str) -> bool:
    if not table_name:
        return False

    if not re.fullmatch(r"u\d+_d\d+_[a-z0-9_]+", table_name):
        print(f"[delete] Skipping unsafe table name: {table_name}")
        return False

    cur.execute(f'DROP TABLE IF EXISTS "{table_name}"')
    return True


def delete_user_file(user_id: int, file_name: str):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, file_type, s3_key
                FROM app_documents
                WHERE user_id = %s
                  AND file_name = %s
                LIMIT 1
            """, (user_id, file_name))

            doc = cur.fetchone()

    if not doc:
        return None

    document_id, file_type, s3_key = doc
    deleted = {}
    structured_table_rows = []

    if file_type == "structured":
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT table_name
                    FROM structured_datasets
                    WHERE user_id = %s
                      AND document_id = %s
                      AND table_name IS NOT NULL

                    UNION

                    SELECT table_name
                    FROM file_upload_events
                    WHERE user_id = %s
                      AND document_id = %s
                      AND table_name IS NOT NULL
                """, (
                    user_id,
                    document_id,
                    user_id,
                    document_id,
                ))

                structured_table_rows = cur.fetchall()

    if s3_key:
        try:
            delete_s3_object(s3_key)
            deleted["s3"] = True
        except Exception as e:
            deleted["s3"] = f"error: {e}"
    else:
        deleted["s3"] = False

    try:
        delete_file_chunks(user_id, file_name)
        deleted["delete_file_chunks"] = True
    except Exception as e:
        deleted["delete_file_chunks"] = f"error: {e}"

    try:
        delete_file_embeddings(user_id, file_name)
        deleted["document_embeddings"] = True
    except Exception as e:
        deleted["document_embeddings"] = f"error: {e}"

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM raw_chunks
                WHERE user_id = %s
                  AND document_id = %s
            """, (user_id, document_id))
            deleted["raw_chunks"] = cur.rowcount

            try:
                cur.execute("""
                    DELETE FROM mart_processed_chunks
                    WHERE user_id = %s
                      AND document_id = %s
                """, (user_id, document_id))
                deleted["mart_processed_chunks"] = cur.rowcount
            except Exception as e:
                conn.rollback()
                deleted["mart_processed_chunks"] = f"skipped/error: {e}"

            try:
                cur.execute("""
                    DELETE FROM document_embeddings
                    WHERE user_id = %s
                      AND document_id = %s
                """, (user_id, document_id))
                deleted["document_embeddings_rows"] = cur.rowcount
            except Exception as e:
                conn.rollback()
                deleted["document_embeddings_rows"] = f"skipped/error: {e}"

            dropped_tables = []

            if file_type == "structured":
                for (table_name,) in structured_table_rows:
                    if safe_drop_table(cur, table_name):
                        dropped_tables.append(table_name)

                cur.execute("""
                    DELETE FROM structured_datasets
                    WHERE user_id = %s
                      AND document_id = %s
                """, (user_id, document_id))

                deleted["structured_datasets"] = cur.rowcount
                deleted["rds_tables_dropped"] = dropped_tables

            cur.execute("""
                DELETE FROM file_upload_events
                WHERE user_id = %s
                  AND document_id = %s
            """, (user_id, document_id))
            deleted["file_upload_events"] = cur.rowcount

            cur.execute("""
                DELETE FROM chat_history
                WHERE user_id = %s
                  AND file_name = %s
            """, (user_id, file_name))
            deleted["chat_history"] = cur.rowcount

            cur.execute("""
                DELETE FROM app_documents
                WHERE user_id = %s
                  AND id = %s
            """, (user_id, document_id))
            deleted["app_documents"] = cur.rowcount


    return {
        "status": "deleted",
        "user_id": user_id,
        "file": file_name,
        "file_type": file_type,
        "document_id": document_id,
        "deleted": deleted,
    }