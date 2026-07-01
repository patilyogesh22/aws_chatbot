import os
import sys
import textwrap
import subprocess
from pathlib import Path

import psycopg2

from app.config import PG_DSN
from app.services.ingestion_service import ingest_file_from_s3_key
from app.services.embedding_service import embed_from_postgres


S3_BUCKET = os.environ["S3_BUCKET"]
S3_KEY = os.environ["S3_KEY"]
USER_ID = int(os.environ["USER_ID"])
DOCUMENT_ID = int(os.environ["DOCUMENT_ID"])
FILE_NAME = os.environ["FILE_NAME"]


def get_file_hash():
    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT file_hash
                FROM app_documents
                WHERE id = %s
                  AND user_id = %s
                LIMIT 1
            """, (
                DOCUMENT_ID,
                USER_ID,
            ))

            row = cur.fetchone()

    if not row or not row[0]:
        raise Exception(f"file_hash not found for document_id={DOCUMENT_ID}")

    return row[0]


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

            cur.execute("""
                UPDATE file_upload_events
                SET status = %s
                WHERE document_id = %s
                  AND user_id = %s
            """, (
                status,
                DOCUMENT_ID,
                USER_ID,
            ))

        conn.commit()

    print(f"[worker] Status changed to {status}")

def create_dbt_profile():
    profile_dir = Path("/root/.dbt")
    profile_dir.mkdir(parents=True, exist_ok=True)

    profile = textwrap.dedent(f"""
    dbt_chatbot:
      target: dev
      outputs:
        dev:
          type: postgres
          host: {os.environ["PG_HOST"]}
          user: {os.environ["PG_USER"]}
          password: {os.environ["PG_PASSWORD"]}
          port: {os.environ["PG_PORT"]}
          dbname: {os.environ["PG_DB"]}
          schema: public
          threads: 4
    """)

    profile_path = profile_dir / "profiles.yml"

    with open(profile_path, "w") as f:
        f.write(profile)

    print(f"[worker] Created dbt profile: {profile_path}")


def run_dbt():
    create_dbt_profile()

    result = subprocess.run(
        [
            "dbt",
            "build",
            "--profiles-dir",
            "/root/.dbt",
            "--select",
            "tag:unstructured",
        ],
        cwd="/app/dbt_chatbot",
        capture_output=True,
        text=True,
        timeout=180,
    )

    print("[dbt stdout]")
    print(result.stdout)

    print("[dbt stderr]")
    print(result.stderr)

    if result.returncode != 0:
        raise Exception("dbt build failed")


def main():
    print("=" * 60)
    print("[worker] Starting ECS unstructured ingestion")
    print("=" * 60)

    print("S3_BUCKET:", S3_BUCKET)
    print("S3_KEY:", S3_KEY)
    print("USER_ID:", USER_ID)
    print("DOCUMENT_ID:", DOCUMENT_ID)
    print("FILE_NAME:", FILE_NAME)

    try:
        mark_status("processing")

        file_hash = get_file_hash()
        print("[worker] file_hash:", file_hash)

        chunks = ingest_file_from_s3_key(
            bucket=S3_BUCKET,
            s3_key=S3_KEY,
            file_name=FILE_NAME,
            file_hash=file_hash,
            user_id=USER_ID,
            document_id=DOCUMENT_ID,
        )

        print(f"[worker] Chunks created: {len(chunks)}")

        should_run_dbt = os.getenv("RUN_DBT", "false").lower() == "true"

        if should_run_dbt:
            print("[worker] RUN_DBT=true, running dbt...")
            run_dbt()
        else:
            print("[worker] RUN_DBT=false, skipping dbt for this file")

        embedded_count = embed_from_postgres(
            user_id=USER_ID,
            file_name=FILE_NAME,
        )

        print(f"[worker] Embeddings created: {embedded_count}")

        mark_status("ready")

        print("=" * 60)
        print("[worker] Completed successfully")
        print("=" * 60)

    except Exception as e:
        print("=" * 60)
        print("[worker] Failed")
        print(str(e))
        print("=" * 60)

        mark_status("error", str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()