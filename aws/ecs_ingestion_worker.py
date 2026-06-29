import os
import sys
import subprocess
import psycopg2

from app.config import PG_DSN
from app.services.ingestion_service import ingest_file_from_s3_key
from app.services.embedding_service import embed_from_postgres


S3_BUCKET = os.environ["S3_BUCKET"]
S3_KEY = os.environ["S3_KEY"]
USER_ID = int(os.environ["USER_ID"])
DOCUMENT_ID = int(os.environ["DOCUMENT_ID"])
FILE_NAME = os.environ["FILE_NAME"]


def mark_status(status: str, error: str | None = None):
    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE app_documents
                SET processing_status = %s,
                    processing_error = %s,
                    updated_at = NOW()
                WHERE id = %s
                  AND user_id = %s
            """, (
                status,
                error,
                DOCUMENT_ID,
                USER_ID,
            ))
        conn.commit()

    print(f"[worker] Status changed to {status}")


def run_dbt():
    result = subprocess.run(
        ["dbt", "build", "--select", "tag:unstructured"],
        cwd="/app/dbt_chatbot",
        capture_output=True,
        text=True,
        timeout=180,
    )

    print("[dbt stdout]", result.stdout[-1000:])
    print("[dbt stderr]", result.stderr[-1000:])

    if result.returncode != 0:
        raise Exception("dbt build failed")


def main():
    print("[worker] Starting ECS unstructured ingestion")
    print("S3_BUCKET:", S3_BUCKET)
    print("S3_KEY:", S3_KEY)
    print("USER_ID:", USER_ID)
    print("DOCUMENT_ID:", DOCUMENT_ID)
    print("FILE_NAME:", FILE_NAME)

    try:
        mark_status("processing")

        chunks = ingest_file_from_s3_key(
            bucket=S3_BUCKET,
            s3_key=S3_KEY,
            file_name=FILE_NAME,
            user_id=USER_ID,
            document_id=DOCUMENT_ID,
        )

        print(f"[worker] Chunks created: {len(chunks)}")

        run_dbt()

        embedded_count = embed_from_postgres(
            user_id=USER_ID,
            file_name=FILE_NAME,
        )

        print(f"[worker] Embeddings created: {embedded_count}")

        mark_status("ready")
        print("[worker] Completed successfully")

    except Exception as e:
        print("[worker] Failed:", str(e))
        mark_status("error", str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()