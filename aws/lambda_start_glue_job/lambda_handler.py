import json
import os
from pathlib import Path

import boto3
import psycopg2


glue = boto3.client("glue")

PG_HOST = os.getenv("PG_HOST")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_PORT = os.getenv("PG_PORT", "5432")

S3_BUCKET = os.getenv("S3_BUCKET")
GLUE_JOB_NAME = os.getenv("GLUE_JOB_NAME", "structured-file-etl-job")
GLUE_DATABASE_NAME = os.getenv("GLUE_DATABASE_NAME")


def get_conn():
    return psycopg2.connect(
        host=PG_HOST,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        port=PG_PORT,
    )


def get_latest_pending_structured_file():
    """
    Pick latest structured file whose crawler has started
    but Glue job has not started yet.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    id,
                    user_id,
                    document_id,
                    file_name,
                    dataset_name,
                    s3_key,
                    bucket_name
                FROM file_upload_events
                WHERE file_type = 'structured'
                  AND status = 'crawler_started'
                ORDER BY id DESC
                LIMIT 1
            """)

            row = cur.fetchone()

    if not row:
        return None

    return {
        "event_id": row[0],
        "user_id": row[1],
        "document_id": row[2] or 0,
        "file_name": row[3],
        "dataset_name": row[4] or Path(row[3]).stem,
        "s3_key": row[5],
        "bucket_name": row[6],
    }


def update_event_status(event_id, status, glue_job_run_id=None, processed_s3_path=None, table_name=None):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE file_upload_events
                SET
                    status = %s,
                    glue_job_run_id = COALESCE(%s, glue_job_run_id),
                    processed_s3_path = COALESCE(%s, processed_s3_path),
                    table_name = COALESCE(%s, table_name)
                WHERE id = %s
            """, (
                status,
                glue_job_run_id,
                processed_s3_path,
                table_name,
                event_id,
            ))

        conn.commit()


def clean_table_name(name):
    return (
        name.lower()
        .replace("-", "_")
        .replace(" ", "_")
        .replace(".", "_")
    )


def lambda_handler(event, context):
    print("🚀 CRAWLER SUCCESS EVENT RECEIVED")
    print(json.dumps(event))

    try:
        latest = get_latest_pending_structured_file()

        if not latest:
            print("No pending structured file found.")
            return {
                "statusCode": 200,
                "body": json.dumps("No pending structured file found")
            }

        event_id = latest["event_id"]
        user_id = latest["user_id"]
        document_id = latest["document_id"]
        file_name = latest["file_name"]
        dataset_name = latest["dataset_name"]
        bucket = latest["bucket_name"] or S3_BUCKET

        table_name = clean_table_name(dataset_name)

        processed_s3_path = (
            f"s3://{bucket}/processed/user_{user_id}/structured/{dataset_name}/"
        )

        print("Starting Glue Job...")
        print("Glue Job:", GLUE_JOB_NAME)
        print("Glue DB:", GLUE_DATABASE_NAME)
        print("Table:", table_name)
        print("Output:", processed_s3_path)

        response = glue.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments={
                "--DATABASE_NAME": GLUE_DATABASE_NAME,
                "--TABLE_NAME": table_name,
                "--S3_OUTPUT_PATH": processed_s3_path,
                "--USER_ID": str(user_id),
                "--DOCUMENT_ID": str(document_id),
                "--FILE_NAME": file_name,
            }
        )

        glue_job_run_id = response["JobRunId"]

        update_event_status(
            event_id=event_id,
            status="glue_job_started",
            glue_job_run_id=glue_job_run_id,
            processed_s3_path=processed_s3_path,
            table_name=table_name,
        )

        print("✅ Glue Job started:", glue_job_run_id)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "status": "glue_job_started",
                "event_id": event_id,
                "glue_job_run_id": glue_job_run_id,
                "table_name": table_name,
                "processed_s3_path": processed_s3_path,
            })
        }

    except Exception as e:
        print("❌ ERROR:", str(e))

        return {
            "statusCode": 500,
            "body": str(e)
        }