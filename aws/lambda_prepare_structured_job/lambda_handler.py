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
    required = {
        "PG_HOST": PG_HOST,
        "PG_DB": PG_DB,
        "PG_USER": PG_USER,
        "PG_PASSWORD": PG_PASSWORD,
    }

    missing = [
        name
        for name, value in required.items()
        if not value
    ]

    if missing:
        raise RuntimeError(
            f"Missing environment variables: {', '.join(missing)}"
        )

    return psycopg2.connect(
        host=PG_HOST,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        port=PG_PORT,
        connect_timeout=8,
    )


def clean_name(value: str) -> str:
    cleaned = Path(value).stem.lower()
    cleaned = re.sub(r"[^a-z0-9_]", "_", cleaned)
    cleaned = re.sub(r"_+", "_", cleaned)
    return cleaned.strip("_")


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
            f"Missing required fields: {', '.join(missing_fields)}"
        )

    if event["file_type"] != "structured":
        raise ValueError(
            "PrepareStructuredJob Lambda only accepts structured files"
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
    dataset_name: str,
    table_name: str,
    iceberg_database: str,
    iceberg_warehouse: str,
) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE app_documents
                SET processing_status = %s,
                    processing_error = NULL,
                    updated_at = NOW()
                WHERE id = %s
                  AND user_id = %s
                """,
                (
                    "preparing_glue_job",
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
                    status = %s,
                    updated_at = NOW()
                WHERE user_id = %s
                  AND document_id = %s
                """,
                (
                    dataset_name,
                    table_name,
                    iceberg_database,
                    table_name,
                    iceberg_warehouse,
                    "preparing_glue_job",
                    user_id,
                    document_id,
                ),
            )

            cur.execute(
                """
                UPDATE file_upload_events
                SET dataset_name = %s,
                    table_name = %s,
                    iceberg_database = %s,
                    iceberg_table = %s,
                    iceberg_s3_path = %s,
                    status = %s,
                    updated_at = NOW()
                WHERE user_id = %s
                  AND document_id = %s
                """,
                (
                    dataset_name,
                    table_name,
                    iceberg_database,
                    table_name,
                    iceberg_warehouse,
                    "preparing_glue_job",
                    user_id,
                    document_id,
                ),
            )

        conn.commit()


def lambda_handler(event, context):
    print("Structured preparation event:")
    print(json.dumps(event))

    try:
        validate_event(event)

        user_id = int(event["user_id"])
        document_id = int(event["document_id"])
        file_name = str(event["file_name"])

        if not document_exists(
            user_id=user_id,
            document_id=document_id,
        ):
            raise ValueError(
                f"Document not found: user_id={user_id}, "
                f"document_id={document_id}"
            )

        dataset_name = event.get("dataset_name") or clean_name(file_name)

        table_name = event.get("table_name") or (
            f"u{user_id}_d{document_id}_{dataset_name}"
        )

        iceberg_database = ICEBERG_DATABASE
        iceberg_warehouse = ICEBERG_WAREHOUSE.rstrip("/") + "/"

        update_processing_metadata(
            user_id=user_id,
            document_id=document_id,
            dataset_name=dataset_name,
            table_name=table_name,
            iceberg_database=iceberg_database,
            iceberg_warehouse=iceberg_warehouse,
        )

        result = {
            **event,
            "user_id": user_id,
            "document_id": document_id,
            "dataset_name": dataset_name,
            "table_name": table_name,
            "iceberg_database": iceberg_database,
            "iceberg_warehouse": iceberg_warehouse,
            "preparation_status": "ready",
        }

        print("Structured job prepared:")
        print(json.dumps(result))

        return result

    except Exception as error:
        print(f"Structured preparation failed: {error}")
        raise