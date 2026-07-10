import json
import os
from typing import Any

import psycopg2


PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")


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
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        connect_timeout=10,
    )


def require_value(
    event: dict[str, Any],
    field_name: str,
) -> Any:
    value = event.get(field_name)

    if value in (None, ""):
        raise ValueError(
            f"Missing required field: {field_name}"
        )

    return value


def get_glue_job_run_id(event: dict[str, Any]) -> str:
    glue_result = event.get("glue_result") or {}

    job_run_id = (
        glue_result.get("Id")
        or glue_result.get("JobRunId")
        or event.get("glue_job_run_id")
    )

    if not job_run_id:
        raise ValueError(
            "Glue JobRunId was not found in the workflow input"
        )

    return str(job_run_id)


def update_success_status(
    *,
    user_id: int,
    document_id: int,
    glue_job_run_id: str,
    iceberg_database: str,
    iceberg_table: str,
    iceberg_s3_path: str,
    step_function_execution_arn: str | None,
) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE app_documents
                SET processing_status = 'completed',
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
                SET status = 'iceberg_ready',
                    glue_job_run_id = %s,
                    iceberg_database = %s,
                    iceberg_table = %s,
                    iceberg_s3_path = %s,
                    step_function_execution_arn = COALESCE(
                        %s,
                        step_function_execution_arn
                    ),
                    error_message = NULL,
                    updated_at = NOW()
                WHERE document_id = %s
                  AND user_id = %s
                """,
                (
                    glue_job_run_id,
                    iceberg_database,
                    iceberg_table,
                    iceberg_s3_path,
                    step_function_execution_arn,
                    document_id,
                    user_id,
                ),
            )

            cur.execute(
                """
                UPDATE file_upload_events
                SET status = 'completed',
                    glue_job_run_id = %s,
                    iceberg_database = %s,
                    iceberg_table = %s,
                    iceberg_s3_path = %s,
                    step_function_execution_arn = COALESCE(
                        %s,
                        step_function_execution_arn
                    ),
                    error_message = NULL,
                    updated_at = NOW()
                WHERE document_id = %s
                  AND user_id = %s
                """,
                (
                    glue_job_run_id,
                    iceberg_database,
                    iceberg_table,
                    iceberg_s3_path,
                    step_function_execution_arn,
                    document_id,
                    user_id,
                ),
            )

        conn.commit()


def lambda_handler(event, context):
    print("Structured success event:")
    print(json.dumps(event, default=str))

    user_id = int(require_value(event, "user_id"))
    document_id = int(require_value(event, "document_id"))

    iceberg_database = str(
        require_value(event, "iceberg_database")
    )

    iceberg_table = str(
        require_value(event, "table_name")
    )

    iceberg_warehouse = str(
        require_value(event, "iceberg_warehouse")
    ).rstrip("/")

    step_function_execution_arn = event.get(
        "step_function_execution_arn"
    )

    glue_job_run_id = get_glue_job_run_id(event)

    iceberg_s3_path = (
        f"{iceberg_warehouse}/"
        f"{iceberg_database}.db/"
        f"{iceberg_table}/"
    )

    update_success_status(
        user_id=user_id,
        document_id=document_id,
        glue_job_run_id=glue_job_run_id,
        iceberg_database=iceberg_database,
        iceberg_table=iceberg_table,
        iceberg_s3_path=iceberg_s3_path,
        step_function_execution_arn=step_function_execution_arn,
    )

    result = {
        "status": "iceberg_ready",
        "user_id": user_id,
        "document_id": document_id,
        "glue_job_run_id": glue_job_run_id,
        "iceberg_database": iceberg_database,
        "iceberg_table": iceberg_table,
        "iceberg_s3_path": iceberg_s3_path,
        "step_function_execution_arn": step_function_execution_arn,
    }

    print("Structured metadata updated successfully:")
    print(json.dumps(result, default=str))

    return result