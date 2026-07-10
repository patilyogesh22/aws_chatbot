from typing import Any

from app.db import get_db_connection


READY_STATUSES = {"iceberg_ready", "completed", "ready"}
PROCESSING_STATUSES = {
    "upload_saved",
    "sqs_queued",
    "step_function_started",
    "preparing_glue_job",
    "glue_job_pending",
    "glue_job_started",
    "processing",
}


def get_user_iceberg_dataset(
    *,
    user_id: int,
    file_name: str,
) -> dict[str, Any]:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    sd.document_id,
                    sd.file_name,
                    sd.dataset_name,
                    COALESCE(sd.status, 'upload_saved') AS status,
                    sd.iceberg_database,
                    sd.iceberg_table,
                    sd.iceberg_s3_path,
                    sd.row_count,
                    sd.column_count,
                    sd.schema_json,
                    sd.sample_json
                FROM structured_datasets sd
                JOIN app_documents ad
                  ON ad.id = sd.document_id
                 AND ad.user_id = sd.user_id
                WHERE sd.user_id = %s
                  AND sd.file_name = %s
                  AND COALESCE(ad.file_type, '') = 'structured'
                ORDER BY sd.id DESC
                LIMIT 1
                """,
                (user_id, file_name),
            )
            row = cur.fetchone()

    if not row:
        raise ValueError(
            f"No structured dataset found for '{file_name}'. "
            "Please upload and process the file first."
        )

    (
        document_id,
        stored_file_name,
        dataset_name,
        status,
        iceberg_database,
        iceberg_table,
        iceberg_s3_path,
        row_count,
        column_count,
        schema_json,
        sample_json,
    ) = row

    if status in PROCESSING_STATUSES:
        raise ValueError(
            f"'{file_name}' is still being processed (status: {status}). "
            "Please wait and try again."
        )

    if status not in READY_STATUSES:
        raise ValueError(
            f"'{file_name}' is not ready for querying (status: {status})."
        )

    if not iceberg_database or not iceberg_table:
        raise ValueError(
            f"Iceberg metadata is missing for '{file_name}'."
        )

    return {
        "document_id": document_id,
        "file_name": stored_file_name,
        "dataset_name": dataset_name,
        "status": status,
        "iceberg_database": iceberg_database,
        "iceberg_table": iceberg_table,
        "iceberg_s3_path": iceberg_s3_path,
        "row_count": row_count or 0,
        "column_count": column_count or 0,
        "schema_json": schema_json or {},
        "sample_json": sample_json or [],
    }