"""File listing, deletion, and processing-status routes."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException

from app.auth import get_current_user
from app.db import get_db_connection
from app.services.file_delete_service import delete_user_file


router = APIRouter()
logger = logging.getLogger(__name__)

READY_DOCUMENT_STATUSES = {
    "ready",
    "completed",
}

READY_STRUCTURED_STATUSES = {
    "ready",
    "completed",
    "iceberg_ready",
}

STATUS_MESSAGES = {
    "upload_saved": "File uploaded. Waiting to be queued…",
    "sqs_queued": "File queued for background processing…",
    "step_function_started": "Workflow started. Processing will begin shortly…",
    "preparing_glue_job": "Preparing the structured Spark job…",
    "processing": "File is being processed…",
    "glue_job_pending": "Waiting for AWS Glue to start…",
    "glue_job_started": "AWS Glue is processing your file…",
    "ready": "File is ready. You can ask questions now.",
    "completed": "File processing completed. You can ask questions now.",
    "iceberg_ready": "Iceberg table is ready. You can ask questions now.",
    "error": "Processing failed. Please re-upload the file.",
    "failed": "Processing failed. Please re-upload the file.",
    "sqs_failed": "Failed to send file to the processing queue.",
    "not_found": "File not found. Please upload it first.",
}


def is_file_ready(
    *,
    file_type: str,
    processing_status: str,
    structured_status: str,
) -> bool:
    """Determine whether a file is ready for chat."""
    if file_type == "structured":
        return structured_status in READY_STRUCTURED_STATUSES

    return processing_status in READY_DOCUMENT_STATUSES


def get_effective_status(
    *,
    file_type: str,
    processing_status: str,
    structured_status: str,
) -> str:
    """Return the status that should be shown to the frontend."""
    if file_type == "structured" and structured_status:
        return structured_status

    return processing_status


@router.get("/files")
def list_files(
    current_user: dict = Depends(get_current_user),
):
    """Return all files owned by the authenticated user."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        d.id,
                        d.file_name,
                        COALESCE(d.file_type, 'unknown') AS file_type,
                        d.file_size,
                        d.uploaded_at,
                        COALESCE(
                            d.processing_status,
                            'upload_saved'
                        ) AS processing_status,
                        d.processing_error,
                        COALESCE(COUNT(r.chunk_id), 0) AS chunk_count,
                        COALESCE(sd.status, '') AS structured_status,
                        sd.iceberg_database,
                        sd.iceberg_table
                    FROM app_documents d
                    LEFT JOIN raw_chunks r
                      ON r.document_id = d.id
                     AND r.user_id = d.user_id
                    LEFT JOIN structured_datasets sd
                      ON sd.document_id = d.id
                     AND sd.user_id = d.user_id
                    WHERE d.user_id = %s
                    GROUP BY
                        d.id,
                        d.file_name,
                        d.file_type,
                        d.file_size,
                        d.uploaded_at,
                        d.processing_status,
                        d.processing_error,
                        sd.status,
                        sd.iceberg_database,
                        sd.iceberg_table
                    ORDER BY d.uploaded_at DESC
                    """,
                    (current_user["id"],),
                )

                rows = cur.fetchall()

        files = []

        for row in rows:
            (
                document_id,
                file_name,
                file_type,
                file_size,
                uploaded_at,
                processing_status,
                processing_error,
                chunk_count,
                structured_status,
                iceberg_database,
                iceberg_table,
            ) = row

            effective_status = get_effective_status(
                file_type=file_type,
                processing_status=processing_status,
                structured_status=structured_status,
            )

            ready = is_file_ready(
                file_type=file_type,
                processing_status=processing_status,
                structured_status=structured_status,
            )

            files.append(
                {
                    "document_id": document_id,
                    "name": file_name,
                    "file_type": file_type,
                    "size": file_size or 0,
                    "uploaded_at": (
                        uploaded_at.isoformat()
                        if uploaded_at
                        else None
                    ),
                    "processing_status": effective_status,
                    "document_status": processing_status,
                    "structured_status": structured_status or None,
                    "processing_error": processing_error,
                    "chunks": chunk_count or 0,
                    "ready": ready,
                    "iceberg_database": iceberg_database,
                    "iceberg_table": iceberg_table,
                }
            )

        return {"files": files}

    except Exception as error:
        logger.exception(
            "Unable to load files for user %s",
            current_user["id"],
        )

        raise HTTPException(
            status_code=500,
            detail="Unable to load files",
        ) from error


@router.delete("/files/{file_name}")
def delete_file(
    file_name: str,
    current_user: dict = Depends(get_current_user),
):
    """
    Delete a user-owned file and all related resources.

    Structured files:
        Iceberg S3 files
        -> Glue Catalog table
        -> raw S3 object
        -> PostgreSQL metadata

    Unstructured files:
        raw S3 object
        -> chunks and embeddings
        -> PostgreSQL metadata
    """
    try:
        result = delete_user_file(
            user_id=current_user["id"],
            file_name=file_name,
        )

    except Exception as error:
        logger.exception(
            "Unexpected file deletion failure: user=%s file=%s",
            current_user["id"],
            file_name,
        )

        raise HTTPException(
            status_code=500,
            detail="File deletion failed due to an internal error",
        ) from error

    if not result:
        raise HTTPException(
            status_code=404,
            detail="File not found",
        )

    if result.get("status") == "partial_failed":
        logger.error(
            "Partial file deletion failure: user=%s file=%s errors=%s",
            current_user["id"],
            file_name,
            result.get("errors"),
        )

        raise HTTPException(
            status_code=502,
            detail=result,
        )

    return result


@router.get("/structured/status/{file_name}")
def structured_status(
    file_name: str,
    current_user: dict = Depends(get_current_user),
):
    """Return the latest processing state for one selected file."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        d.file_type,
                        COALESCE(
                            d.processing_status,
                            'upload_saved'
                        ) AS processing_status,
                        d.processing_error,
                        COALESCE(sd.status, '') AS structured_status,
                        sd.iceberg_table,
                        sd.row_count,
                        sd.glue_job_run_id,
                        sd.iceberg_database,
                        sd.iceberg_s3_path,
                        COALESCE(
                            sd.updated_at,
                            d.updated_at,
                            d.uploaded_at
                        ) AS updated_at
                    FROM app_documents d
                    LEFT JOIN structured_datasets sd
                      ON sd.document_id = d.id
                     AND sd.user_id = d.user_id
                    WHERE d.user_id = %s
                      AND d.file_name = %s
                    ORDER BY d.id DESC
                    LIMIT 1
                    """,
                    (
                        current_user["id"],
                        file_name,
                    ),
                )

                row = cur.fetchone()

    except Exception as error:
        logger.exception(
            "Unable to load status: user=%s file=%s",
            current_user["id"],
            file_name,
        )

        raise HTTPException(
            status_code=500,
            detail="Unable to load file status",
        ) from error

    if not row:
        return {
            "file_name": file_name,
            "status": "not_found",
            "ready": False,
            "message": STATUS_MESSAGES["not_found"],
        }

    (
        file_type,
        processing_status,
        processing_error,
        structured_status_value,
        iceberg_table,
        row_count,
        glue_job_run_id,
        iceberg_database,
        iceberg_s3_path,
        updated_at,
    ) = row

    status = get_effective_status(
        file_type=file_type,
        processing_status=processing_status,
        structured_status=structured_status_value,
    )

    ready = is_file_ready(
        file_type=file_type,
        processing_status=processing_status,
        structured_status=structured_status_value,
    )

    return {
        "file_name": file_name,
        "file_type": file_type,
        "status": status,
        "document_status": processing_status,
        "structured_status": structured_status_value or None,
        "ready": ready,
        "table_name": iceberg_table,
        "iceberg_database": iceberg_database,
        "iceberg_s3_path": iceberg_s3_path,
        "row_count": row_count or 0,
        "glue_job_run_id": glue_job_run_id,
        "processing_error": processing_error,
        "updated_at": (
            updated_at.isoformat()
            if updated_at
            else None
        ),
        "message": (
            processing_error
            or STATUS_MESSAGES.get(
                status,
                f"Status: {status}",
            )
        ),
    }