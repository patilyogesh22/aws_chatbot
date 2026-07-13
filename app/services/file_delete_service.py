"""Delete uploaded files and all related application/Lakehouse resources."""

from __future__ import annotations

import os
from typing import Any
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError

from app.db import get_db_connection
from aws.s3_ingestion import delete_s3_object


AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
DEFAULT_S3_BUCKET = os.getenv("S3_BUCKET")

glue_client = boto3.client(
    "glue",
    region_name=AWS_REGION,
)

s3_client = boto3.client(
    "s3",
    region_name=AWS_REGION,
)


class FileDeletionError(RuntimeError):
    """Raised when a file cannot be safely deleted."""


def parse_s3_uri(s3_uri: str | None) -> tuple[str | None, str | None]:
    """
    Convert an S3 URI into bucket and key.

    Example:
        s3://my-bucket/lakehouse/iceberg/table/
        -> ("my-bucket", "lakehouse/iceberg/table/")
    """
    if not s3_uri:
        return None, None

    parsed = urlparse(s3_uri)

    if parsed.scheme != "s3" or not parsed.netloc:
        raise FileDeletionError(
            f"Invalid S3 URI stored for dataset: {s3_uri}"
        )

    return parsed.netloc, parsed.path.lstrip("/")


def delete_s3_prefix(
    *,
    bucket: str,
    prefix: str,
) -> int:
    """
    Delete every object beneath one S3 prefix.

    S3 does not contain real folders, so all matching objects must be
    listed and deleted.
    """
    if not bucket or not prefix:
        return 0

    normalized_prefix = prefix.lstrip("/")

    paginator = s3_client.get_paginator("list_objects_v2")
    deleted_count = 0

    for page in paginator.paginate(
        Bucket=bucket,
        Prefix=normalized_prefix,
    ):
        objects = page.get("Contents", [])

        if not objects:
            continue

        # S3 DeleteObjects supports at most 1,000 objects per request.
        for start in range(0, len(objects), 1000):
            batch = objects[start : start + 1000]

            response = s3_client.delete_objects(
                Bucket=bucket,
                Delete={
                    "Objects": [
                        {"Key": item["Key"]}
                        for item in batch
                    ],
                    "Quiet": True,
                },
            )

            errors = response.get("Errors", [])

            if errors:
                raise FileDeletionError(
                    "Some Iceberg S3 objects could not be deleted: "
                    f"{errors[:3]}"
                )

            deleted_count += len(batch)

    return deleted_count


def delete_glue_table(
    *,
    database_name: str | None,
    table_name: str | None,
) -> bool:
    """
    Remove an Iceberg table entry from the AWS Glue Data Catalog.

    This removes catalog metadata only. The S3 table files are deleted
    separately by delete_s3_prefix().
    """
    if not database_name or not table_name:
        return False

    try:
        glue_client.delete_table(
            DatabaseName=database_name,
            Name=table_name,
        )
        return True

    except glue_client.exceptions.EntityNotFoundException:
        # The catalog entry is already absent, so deletion is effectively done.
        return False

    except ClientError as error:
        raise FileDeletionError(
            "Failed to delete the Glue Catalog table "
            f"{database_name}.{table_name}: {error}"
        ) from error


def get_file_deletion_metadata(
    *,
    user_id: int,
    file_name: str,
) -> dict[str, Any] | None:
    """
    Read all metadata required before deleting external resources.

    External metadata must be read before PostgreSQL rows are removed,
    otherwise the Glue table and Iceberg path cannot be located.
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    d.id,
                    d.file_name,
                    COALESCE(d.file_type, 'unknown') AS file_type,
                    d.s3_key,
                    fue.bucket_name,
                    sd.iceberg_database,
                    sd.iceberg_table,
                    sd.iceberg_s3_path
                FROM app_documents d
                LEFT JOIN file_upload_events fue
                  ON fue.document_id = d.id
                 AND fue.user_id = d.user_id
                LEFT JOIN structured_datasets sd
                  ON sd.document_id = d.id
                 AND sd.user_id = d.user_id
                WHERE d.user_id = %s
                  AND d.file_name = %s
                ORDER BY d.id DESC
                LIMIT 1
                """,
                (
                    user_id,
                    file_name,
                ),
            )

            row = cur.fetchone()

    if not row:
        return None

    (
        document_id,
        stored_file_name,
        file_type,
        s3_key,
        bucket_name,
        iceberg_database,
        iceberg_table,
        iceberg_s3_path,
    ) = row

    return {
        "document_id": document_id,
        "file_name": stored_file_name,
        "file_type": file_type,
        "s3_key": s3_key,
        "bucket_name": bucket_name or DEFAULT_S3_BUCKET,
        "iceberg_database": iceberg_database,
        "iceberg_table": iceberg_table,
        "iceberg_s3_path": iceberg_s3_path,
    }


def delete_database_records(
    *,
    user_id: int,
    document_id: int,
    file_name: str,
    is_structured: bool,
) -> dict[str, int]:
    """
    Delete all PostgreSQL rows for one document.

    Deletes use document_id whenever possible because it is safer than
    deleting by filename.
    """
    deleted: dict[str, int] = {}

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Query cache can contain structured Athena results.
            cur.execute(
                """
                DELETE FROM query_cache
                WHERE user_id = %s
                  AND document_id = %s
                """,
                (
                    user_id,
                    document_id,
                ),
            )
            deleted["query_cache"] = cur.rowcount

            # Unstructured records may be absent for structured documents.
            cur.execute(
                """
                DELETE FROM document_embeddings
                WHERE user_id = %s
                  AND document_id = %s
                """,
                (
                    user_id,
                    document_id,
                ),
            )
            deleted["document_embeddings"] = cur.rowcount

            cur.execute(
                """
                DELETE FROM mart_processed_chunks
                WHERE user_id = %s
                  AND document_id = %s
                """,
                (
                    user_id,
                    document_id,
                ),
            )
            deleted["mart_processed_chunks"] = cur.rowcount

            cur.execute(
                """
                DELETE FROM raw_chunks
                WHERE user_id = %s
                  AND document_id = %s
                """,
                (
                    user_id,
                    document_id,
                ),
            )
            deleted["raw_chunks"] = cur.rowcount

            cur.execute(
                """
                DELETE FROM chat_history
                WHERE user_id = %s
                  AND file_name = %s
                """,
                (
                    user_id,
                    file_name,
                ),
            )
            deleted["chat_history"] = cur.rowcount

            if is_structured:
                cur.execute(
                    """
                    DELETE FROM structured_datasets
                    WHERE user_id = %s
                      AND document_id = %s
                    """,
                    (
                        user_id,
                        document_id,
                    ),
                )
                deleted["structured_datasets"] = cur.rowcount

            cur.execute(
                """
                DELETE FROM file_upload_events
                WHERE user_id = %s
                  AND document_id = %s
                """,
                (
                    user_id,
                    document_id,
                ),
            )
            deleted["file_upload_events"] = cur.rowcount

            cur.execute(
                """
                DELETE FROM app_documents
                WHERE user_id = %s
                  AND id = %s
                """,
                (
                    user_id,
                    document_id,
                ),
            )
            deleted["app_documents"] = cur.rowcount

    return deleted


def delete_user_file(
    user_id: int,
    file_name: str,
) -> dict[str, Any] | None:
    """
    Delete one user-owned file and all related resources.

    Structured deletion:
        Glue Catalog table
        -> Iceberg S3 objects
        -> raw S3 object
        -> PostgreSQL metadata

    Unstructured deletion:
        raw S3 object
        -> chunks/embeddings
        -> PostgreSQL metadata
    """
    metadata = get_file_deletion_metadata(
        user_id=user_id,
        file_name=file_name,
    )

    if not metadata:
        return None

    document_id = metadata["document_id"]
    file_type = metadata["file_type"]
    is_structured = file_type == "structured"

    deleted: dict[str, Any] = {
        "glue_catalog_table": False,
        "iceberg_s3_objects": 0,
        "raw_s3_object": False,
    }

    external_errors: list[str] = []

    # ---------------------------------------------------------
    # Structured Lakehouse cleanup
    # ---------------------------------------------------------
    if is_structured:
        try:
            deleted["glue_catalog_table"] = delete_glue_table(
                database_name=metadata["iceberg_database"],
                table_name=metadata["iceberg_table"],
            )
        except Exception as error:
            external_errors.append(str(error))

        try:
            iceberg_bucket, iceberg_prefix = parse_s3_uri(
                metadata["iceberg_s3_path"]
            )

            if iceberg_bucket and iceberg_prefix:
                deleted["iceberg_s3_objects"] = delete_s3_prefix(
                    bucket=iceberg_bucket,
                    prefix=iceberg_prefix,
                )
        except Exception as error:
            external_errors.append(str(error))

    # ---------------------------------------------------------
    # Raw uploaded object cleanup
    # ---------------------------------------------------------
    if metadata["s3_key"]:
        try:
            # Existing helper uses the configured project bucket.
            delete_s3_object(metadata["s3_key"])
            deleted["raw_s3_object"] = True
        except Exception as error:
            external_errors.append(
                f"Failed to delete raw S3 object: {error}"
            )

    # Do not remove database metadata if AWS cleanup failed.
    # Keeping metadata allows the operation to be retried safely.
    if external_errors:
        return {
            "status": "partial_failed",
            "user_id": user_id,
            "file": file_name,
            "file_type": file_type,
            "document_id": document_id,
            "deleted": deleted,
            "errors": external_errors,
            "message": (
                "Some AWS resources could not be deleted. "
                "PostgreSQL metadata was kept so deletion can be retried."
            ),
        }

    deleted["database"] = delete_database_records(
        user_id=user_id,
        document_id=document_id,
        file_name=file_name,
        is_structured=is_structured,
    )

    return {
        "status": "deleted",
        "user_id": user_id,
        "file": file_name,
        "file_type": file_type,
        "document_id": document_id,
        "deleted": deleted,
    }