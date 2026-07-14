import json
import os
import re
from pathlib import Path
from typing import Any
import psycopg2


PG_HOST = os.getenv("PG_HOST")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_PORT = os.getenv("PG_PORT", "5432")

ICEBERG_DATABASE = os.getenv(
    "ICEBERG_DATABASE",
    "chatbot_lakehouse",
)

ICEBERG_WAREHOUSE = os.getenv(
    "ICEBERG_WAREHOUSE",
    "s3://chatbot-documents-815512685419-eu-north-1-an/lakehouse/iceberg/",
)


def get_connection():
    required_variables = {
        "PG_HOST": PG_HOST,
        "PG_DB": PG_DB,
        "PG_USER": PG_USER,
        "PG_PASSWORD": PG_PASSWORD,
    }

    missing_variables = [
        name
        for name, value in required_variables.items()
        if not value
    ]

    if missing_variables:
        raise RuntimeError(
            "Missing environment variables: "
            + ", ".join(missing_variables)
        )

    return psycopg2.connect(
        host=PG_HOST,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        port=PG_PORT,
        connect_timeout=10,
    )


def clean_name(value: str) -> str:
    cleaned = Path(value).stem.lower()
    cleaned = re.sub(r"[^a-z0-9_]", "_", cleaned)
    cleaned = re.sub(r"_+", "_", cleaned)
    cleaned = cleaned.strip("_")

    return cleaned or "dataset"


def validate_event(event: dict[str, Any]) -> None:
    required_fields = [
        "user_id",
        "document_id",
        "file_name",
        "file_type",
        "bucket",
        "s3_key",
        "s3_path",
    ]

    missing_fields = [
        field
        for field in required_fields
        if event.get(field) in (None, "")
    ]

    if missing_fields:
        raise ValueError(
            "Missing required fields: "
            + ", ".join(missing_fields)
        )

    if event["file_type"] != "structured":
        raise ValueError(
            "This Lambda accepts only structured files"
        )

def validate_iceberg_configuration() -> None:
    if not ICEBERG_DATABASE:
        raise RuntimeError("ICEBERG_DATABASE is not configured")

    if not ICEBERG_WAREHOUSE:
        raise RuntimeError("ICEBERG_WAREHOUSE is not configured")

    if not ICEBERG_WAREHOUSE.startswith("s3://"):
        raise RuntimeError(
            "ICEBERG_WAREHOUSE must be a valid s3:// URI"
        )

def document_exists(
    *,
    user_id: int,
    document_id: int,
) -> bool:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM app_documents
                WHERE id = %s
                  AND user_id = %s
                LIMIT 1
                """,
                (
                    document_id,
                    user_id,
                ),
            )

            return cur.fetchone() is not None


def update_processing_metadata(
    *,
    user_id: int,
    document_id: int,
    bucket_name: str,
    dataset_name: str,
    table_name: str,
    iceberg_database: str,
    iceberg_warehouse: str,
    step_function_execution_arn: str | None,
) -> None:
    iceberg_s3_path = (
        f"{iceberg_warehouse.rstrip('/')}/"
        f"{iceberg_database}.db/"
        f"{table_name}/"
    )

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE app_documents
                SET processing_status = 'preparing_glue_job',
                    processing_error = NULL,
                    updated_at = NOW()
                WHERE id = %s
                  AND user_id = %s
                """,
                (
                    document_id,
                    user_id,
                ),
            )

            cur.execute(
                """
                UPDATE structured_datasets
                SET dataset_name = %s,
                    table_name = %s,
                    iceberg_database = %s,
                    iceberg_table = %s,
                    iceberg_s3_path = %s,
                    step_function_execution_arn = %s,
                    status = 'preparing_glue_job',
                    error_message = NULL,
                    updated_at = NOW()
                WHERE user_id = %s
                  AND document_id = %s
                """,
                (
                    dataset_name,
                    table_name,
                    iceberg_database,
                    table_name,
                    iceberg_s3_path,
                    step_function_execution_arn,
                    user_id,
                    document_id,
                ),
            )

            cur.execute(
                """
                UPDATE file_upload_events
                SET bucket_name = %s,
                    dataset_name = %s,
                    table_name = %s,
                    iceberg_database = %s,
                    iceberg_table = %s,
                    iceberg_s3_path = %s,
                    step_function_execution_arn = %s,
                    status = 'preparing_glue_job',
                    error_message = NULL,
                    updated_at = NOW()
                WHERE user_id = %s
                  AND document_id = %s
                """,
                (
                    bucket_name,
                    dataset_name,
                    table_name,
                    iceberg_database,
                    table_name,
                    iceberg_s3_path,
                    step_function_execution_arn,
                    user_id,
                    document_id,
                ),
            )

        conn.commit()



def lambda_handler(event, context):
    print("Structured preparation event:")
    print(json.dumps(event, default=str))

    validate_event(event)

    validate_iceberg_configuration()

    user_id = int(event["user_id"])
    document_id = int(event["document_id"])
    file_name = str(event["file_name"])
    bucket_name = str(event["bucket"])

    if not document_exists(
        user_id=user_id,
        document_id=document_id,
    ):
        raise ValueError(
            f"Document not found: user_id={user_id}, "
            f"document_id={document_id}"
        )


    dataset_name = event.get("dataset_name")
    table_name = event.get("table_name")

    if not dataset_name:
        dataset_name = clean_name(file_name)

    if not table_name:
        table_name = f"u{user_id}_d{document_id}_{dataset_name}"
    iceberg_database = ICEBERG_DATABASE.strip().lower()
    iceberg_warehouse = ICEBERG_WAREHOUSE.rstrip("/") + "/"

    step_function_execution_arn = event.get(
        "step_function_execution_arn"
    )

    update_processing_metadata(
        user_id=user_id,
        document_id=document_id,
        bucket_name=bucket_name,
        dataset_name=dataset_name,
        table_name=table_name,
        iceberg_database=iceberg_database,
        iceberg_warehouse=iceberg_warehouse,
        step_function_execution_arn=step_function_execution_arn,
    )

    result = {
        **event,
        "user_id": user_id,
        "document_id": document_id,
        "dataset_name": dataset_name,
        "table_name": table_name,
        "iceberg_database": iceberg_database,
        "iceberg_warehouse": iceberg_warehouse,
        "step_function_execution_arn": step_function_execution_arn,
        "preparation_status": "ready",
    }

    print("Structured job prepared:")
    print(json.dumps(result, default=str))

    return result