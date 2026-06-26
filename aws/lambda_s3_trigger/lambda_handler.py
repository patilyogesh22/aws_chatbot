import json
import os
import re
import time
from pathlib import Path

import boto3
import psycopg2


# -------------------------
# AWS CLIENTS
# -------------------------
stepfunctions = boto3.client(
    "stepfunctions",
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

STEP_FUNCTION_ARN = os.getenv("STEP_FUNCTION_ARN")


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


def clean_table_name(name: str):
    name = Path(name).stem.lower()
    name = re.sub(r"[^a-z0-9_]", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_")


def document_exists(user_id, document_id):
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
    status="step_function_started",
):
    dataset_name = clean_table_name(file_name)

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

        conn.commit()


def start_step_function(payload):
    if not STEP_FUNCTION_ARN:
        raise Exception("STEP_FUNCTION_ARN environment variable is not set")

    execution_name = f"doc-{payload['document_id']}-{int(time.time())}"

    response = stepfunctions.start_execution(
        stateMachineArn=STEP_FUNCTION_ARN,
        name=execution_name,
        input=json.dumps(payload),
    )

    print("Step Function started:", response["executionArn"])
    return response["executionArn"]


def lambda_handler(event, context):
    """
    Trigger:
    SQS → Lambda → Step Functions → Glue
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
                    "s3_key": key,
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
                    status="skipped_until_ecs_phase",
                )

                results.append({
                    "file_name": file_name,
                    "file_type": file_type,
                    "status": "skipped_until_ecs_phase",
                })

                continue

            dataset_name = clean_table_name(file_name)
            table_name = f"u{int(user_id)}_d{int(document_id)}_{dataset_name}"

            payload = {
                "user_id": user_id,
                "document_id": document_id,
                "file_name": file_name,
                "file_type": file_type,
                "bucket": bucket,
                "s3_key": key,
                "s3_path": f"s3://{bucket}/{key}",
                "file_size": size,
                "dataset_name": dataset_name,
                "table_name": table_name,
            }
            print("About to start Step Function")
            print("STEP_FUNCTION_ARN =", STEP_FUNCTION_ARN)
            execution_arn = start_step_function(payload)
            print("Step Function started:", execution_arn)

            store_upload_event(
                user_id=user_id,
                file_name=file_name,
                key=key,
                bucket=bucket,
                size=size,
                file_type=file_type,
                document_id=document_id,
                table_name=table_name,
                glue_job_run_id=execution_arn,
                status="step_function_started",
            )

            update_structured_dataset(
                user_id=user_id,
                document_id=document_id,
                file_name=file_name,
                s3_key=key,
                dataset_name=dataset_name,
                table_name=table_name,
                glue_job_run_id=execution_arn,
                status="step_function_started",
            )

            results.append({
                "file_name": file_name,
                "file_type": file_type,
                "dataset_name": dataset_name,
                "table_name": table_name,
                "execution_arn": execution_arn,
                "status": "step_function_started",
            })

        except Exception as e:
            print("Error processing SQS message:", str(e))
            raise

    return {
        "statusCode": 200,
        "body": json.dumps({
            "status": "success",
            "results": results,
        }),
    }