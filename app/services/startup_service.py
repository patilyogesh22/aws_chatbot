import psycopg2

from app.config import PG_DSN
from app.auth import init_auth_tables
from app.services.ingestion_service import init_postgres
from app.services.embedding_service import init_pgvector


def startup():
    init_auth_tables()
    init_postgres()
    init_pgvector()
    init_extra_tables()


def init_extra_tables():
    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS structured_datasets (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    document_id INTEGER,
                    file_name TEXT NOT NULL,
                    raw_s3_key TEXT,
                    table_name TEXT,
                    dataset_name TEXT,
                    glue_job_run_id TEXT,
                    schema_json JSONB,
                    sample_json JSONB,
                    row_count INTEGER,
                    status TEXT DEFAULT 'glue_job_pending',
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS file_upload_events (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER,
                    file_name TEXT,
                    s3_key TEXT,
                    bucket_name TEXT,
                    file_size BIGINT,
                    file_type TEXT,
                    document_id INTEGER,
                    dataset_name TEXT,
                    status TEXT,
                    table_name TEXT,
                    glue_job_run_id TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)

            migrations = [
                "ALTER TABLE structured_datasets ADD COLUMN IF NOT EXISTS table_name TEXT",
                "ALTER TABLE structured_datasets ADD COLUMN IF NOT EXISTS schema_json JSONB",
                "ALTER TABLE structured_datasets ADD COLUMN IF NOT EXISTS sample_json JSONB",
                "ALTER TABLE structured_datasets ADD COLUMN IF NOT EXISTS row_count INTEGER",
                "ALTER TABLE structured_datasets ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()",
                "ALTER TABLE structured_datasets ADD COLUMN IF NOT EXISTS dataset_name TEXT",
                "ALTER TABLE structured_datasets ADD COLUMN IF NOT EXISTS glue_job_run_id TEXT",

                "ALTER TABLE app_documents ADD COLUMN IF NOT EXISTS file_type TEXT DEFAULT 'unstructured'",
                "ALTER TABLE app_documents ADD COLUMN IF NOT EXISTS s3_key TEXT",
                "ALTER TABLE app_documents ADD COLUMN IF NOT EXISTS file_size BIGINT",

                "ALTER TABLE chat_history ADD COLUMN IF NOT EXISTS generated_sql TEXT",
                "ALTER TABLE chat_history ADD COLUMN IF NOT EXISTS table_name TEXT",
                "ALTER TABLE chat_history ADD COLUMN IF NOT EXISTS file_type TEXT",
                "ALTER TABLE chat_history ADD COLUMN IF NOT EXISTS result_rows JSONB DEFAULT '[]'::jsonb",
                "ALTER TABLE chat_history ADD COLUMN IF NOT EXISTS result_columns JSONB DEFAULT '[]'::jsonb",
                "ALTER TABLE chat_history ADD COLUMN IF NOT EXISTS row_count INTEGER",

                "ALTER TABLE file_upload_events ADD COLUMN IF NOT EXISTS table_name TEXT",
                "ALTER TABLE file_upload_events ADD COLUMN IF NOT EXISTS glue_job_run_id TEXT",
                "ALTER TABLE file_upload_events ADD COLUMN IF NOT EXISTS document_id INTEGER",
            ]

            for sql in migrations:
                cur.execute(sql)

            cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS ux_structured_datasets_user_document
                ON structured_datasets(user_id, document_id)
                WHERE document_id IS NOT NULL
            """)

        conn.commit()