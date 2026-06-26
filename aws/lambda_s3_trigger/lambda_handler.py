import json
import os
import re
from pathlib import Path
import time
import boto3
import psycopg2
from botocore.exceptions import ClientError
# -------------------------
# AWS CLIENTS
# -------------------------
glue = boto3.client(
    "glue",
    region_name=os.getenv("AWS_REGION", "eu-north-1")
)

# -------------------------
# ENV VARIABLES
# -------------------------
PG_HOST = os.getenv("PG_HOST")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_PORT = os.getenv("PG_PORT", "5432")

GLUE_JOB_NAME = os.getenv("GLUE_JOB_NAME", "structured-file-etl-job")


# -------------------------
# DB CONNECTION
# -------------------------
def get_conn():
    return psycopg2.connect(
        host=PG_HOST,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        port=PG_PORT,
    )


# -------------------------
# HELPERS
# -------------------------
def extract_user_id(key: str):
    # uploads/user_1/structured/file.csv
    match = re.search(r"uploads/user_(\d+)/", key)
    return int(match.group(1)) if match else None


def classify_file(file_name: str):
    ext = Path(file_name).suffix.lower()

    if ext in {".csv", ".json", ".parquet",".xlsx",".xls"}:
        return "structured"

    if ext in {".pdf", ".docx", ".txt", ".md", ".pptx"}:
        return "unstructured"

    return "unknown"


def clean_table_name(name: str):
    name = Path(name).stem.lower()
    name = re.sub(r"[^a-z0-9_]", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_")


def get_document_id(user_id, s3_key):
    if not user_id:
        return 0

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id
                    FROM app_documents
                    WHERE user_id = %s
                      AND s3_key = %s
                    ORDER BY id DESC
                    LIMIT 1
                """, (user_id, s3_key))

                row = cur.fetchone()
                return row[0] if row else 0

    except Exception as e:
        print("Could not fetch document_id:", str(e))
        return 0

def get_document_id_with_retry(user_id, s3_key, retries=6, delay=1):
    """
    S3 event may reach Lambda before FastAPI finishes inserting app_documents.
    Retry a few times before failing.
    """
    for attempt in range(1, retries + 1):
        document_id = get_document_id(user_id, s3_key)

        if document_id:
            print("Found document_id:", document_id)
            return document_id

        print(f"Document not found yet. Retry {attempt}/{retries}")
        time.sleep(delay)

    return None
# -------------------------
# DB METADATA UPDATES
# -------------------------
def store_upload_event(
    user_id,
    file_name,
    key,
    bucket,
    size,
    file_type,
    document_id,
    table_name=None,
    glue_job_run_id=None,
):
    dataset_name = clean_table_name(file_name)
    status = "glue_job_started" if glue_job_run_id else "uploaded"

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE file_upload_events
                SET
                    bucket_name = %s,
                    file_size = %s,
                    file_type = %s,
                    dataset_name = %s,
                    status = %s,
                    table_name = %s,
                    glue_job_run_id = %s
                WHERE document_id = %s
            """, (
                bucket,
                size,
                file_type,
                dataset_name,
                status,
                table_name,
                glue_job_run_id,
                document_id,
            ))

            if cur.rowcount == 0:
                cur.execute("""
                    INSERT INTO file_upload_events
                    (
                        user_id,
                        file_name,
                        s3_key,
                        bucket_name,
                        file_size,
                        file_type,
                        document_id,
                        dataset_name,
                        status,
                        table_name,
                        glue_job_run_id
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    user_id,
                    file_name,
                    key,
                    bucket,
                    size,
                    file_type,
                    document_id,
                    dataset_name,
                    status,
                    table_name,
                    glue_job_run_id,
                ))

        conn.commit()
def update_structured_dataset(
    user_id,
    document_id,
    file_name,
    s3_key,
    dataset_name,
    table_name,
    glue_job_run_id,
    status,
):
    """
    Updates metadata row created by FastAPI in structured_datasets.
    This runs after Lambda starts the Glue Job.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE structured_datasets
                SET
                    dataset_name = %s,
                    table_name = %s,
                    glue_job_run_id = %s,
                    status = %s,
                    updated_at = NOW()
                WHERE user_id = %s
                  AND document_id = %s
            """, (
                dataset_name,
                table_name,
                glue_job_run_id,
                status,
                user_id,
                document_id,
            ))

            # Safety insert if FastAPI row was not found
            if cur.rowcount == 0:
                cur.execute("""
                    INSERT INTO structured_datasets
                    (
                        user_id,
                        document_id,
                        file_name,
                        dataset_name,
                        table_name,
                        raw_s3_key,
                        glue_job_run_id,
                        status,
                        created_at,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (user_id, document_id)
                    DO UPDATE SET
                        dataset_name = EXCLUDED.dataset_name,
                        table_name = EXCLUDED.table_name,
                        raw_s3_key = EXCLUDED.raw_s3_key,
                        glue_job_run_id = EXCLUDED.glue_job_run_id,
                        status = EXCLUDED.status,
                        updated_at = NOW();
                """, (
                    user_id,
                    document_id,
                    file_name,
                    dataset_name,
                    table_name,
                    s3_key,
                    glue_job_run_id,
                    status,
                ))

        conn.commit()


# -------------------------
# GLUE JOB
# -------------------------
def start_glue_job(bucket, key, user_id, document_id, file_name):
    dataset_name = clean_table_name(file_name)

    # Unique table per user + document
    table_name = f"u{int(user_id)}_d{int(document_id)}_{dataset_name}"

    s3_input_path = f"s3://{bucket}/{key}"

    print("Starting Glue Job...")
    print("Glue Job:", GLUE_JOB_NAME)
    print("Input:", s3_input_path)
    print("Dataset:", dataset_name)
    print("Table:", table_name)

    try:
        response = glue.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments={
                "--S3_INPUT_PATH": s3_input_path,
                "--TABLE_NAME": table_name,
                "--USER_ID": str(user_id or 0),
                "--DOCUMENT_ID": str(document_id or 0),
                "--FILE_NAME": file_name,
            }
        )

        return dataset_name, table_name, response["JobRunId"]

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")

        if error_code == "ConcurrentRunsExceededException":
            print("Glue is busy. Will retry message later from SQS.")

            # Do not mark failed permanently.
            # Raise error so Lambda fails this message.
            # SQS will retry after visibility timeout.
            raise

        print("Glue StartJobRun failed:", str(e))
        raise

def document_exists(user_id, document_id):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 1
                    FROM app_documents
                    WHERE id = %s
                      AND user_id = %s
                    LIMIT 1
                """, (document_id, user_id))

                return cur.fetchone() is not None

    except Exception as e:
        print("Error checking document existence:", str(e))
        raise
# -------------------------
# LAMBDA HANDLER
# -------------------------

def lambda_handler(event, context):
    """
    Trigger:
    SQS → Lambda → Glue

    This handler safely skips invalid/test messages and processes only valid
    FastAPI file-processing messages.
    """

    print("SQS EVENT RECEIVED")
    print(json.dumps(event))

    results = []

    for record in event.get("Records", []):
        try:
            body = json.loads(record.get("body", "{}"))

            print("SQS BODY:", json.dumps(body))

            required_keys = [
                "bucket",
                "s3_key",
                "file_name",
                "user_id",
                "document_id",
                "file_type",
            ]

            missing_keys = [key for key in required_keys if key not in body]

            if missing_keys:
                print("Invalid SQS message. Missing keys:", missing_keys)
                print("Skipping message:", body)

                results.append({
                    "status": "skipped_invalid_message",
                    "missing_keys": missing_keys,
                    "body": body,
                })

                continue

            bucket = body["bucket"]
            key = body["s3_key"]
            size = body.get("file_size", 0)

            file_name = body["file_name"]
            user_id = int(body["user_id"])
            document_id = int(body["document_id"])
            file_type = body["file_type"]

            print("File:", file_name)
            print("User ID:", user_id)
            print("File type:", file_type)
            print("Document ID:", document_id)
            print("S3 Key:", key)

            if not document_exists(user_id, document_id):
                print(f"Document was deleted. Skipping message. document_id={document_id}")

                results.append({
                    "status": "skipped_deleted_document",
                    "document_id": document_id,
                    "file_name": file_name,
                    "s3_key": key
                })

                continue

            if not document_id:
                raise Exception(
                    f"Missing document_id in SQS message for user_id={user_id}, s3_key={key}"
                )

            if file_type == "unknown":
                print("Unsupported file type. Skipping:", file_name)

                results.append({
                    "file_name": file_name,
                    "file_type": file_type,
                    "status": "skipped_unknown_file"
                })

                continue

            if file_type != "structured":
                store_upload_event(
                    user_id=user_id,
                    file_name=file_name,
                    key=key,
                    bucket=bucket,
                    size=size,
                    file_type=file_type,
                    document_id=document_id,
                )

                results.append({
                    "file_name": file_name,
                    "file_type": file_type,
                    "status": "skipped_glue_job_until_ecs_phase"
                })

                continue

            dataset_name, table_name, glue_job_run_id = start_glue_job(
                bucket=bucket,
                key=key,
                user_id=user_id,
                document_id=document_id,
                file_name=file_name,
            )

            store_upload_event(
                user_id=user_id,
                file_name=file_name,
                key=key,
                bucket=bucket,
                size=size,
                file_type=file_type,
                document_id=document_id,
                table_name=table_name,
                glue_job_run_id=glue_job_run_id,
            )

            update_structured_dataset(
                user_id=user_id,
                document_id=document_id,
                file_name=file_name,
                s3_key=key,
                dataset_name=dataset_name,
                table_name=table_name,
                glue_job_run_id=glue_job_run_id,
                status="glue_job_started",
            )

            results.append({
                "file_name": file_name,
                "file_type": file_type,
                "dataset_name": dataset_name,
                "table_name": table_name,
                "glue_job_run_id": glue_job_run_id,
                "status": "glue_job_started"
            })

        except Exception as e:
            print("Error processing SQS message:", str(e))
            raise

    return {
        "statusCode": 200,
        "body": json.dumps({
            "status": "success",
            "results": results
        })
    }