"""Delete uploaded files and all related AWS and PostgreSQL resources."""

from __future__ import annotations

import logging
import os
from typing import Any
from urllib.parse import urlparse

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from app.db import get_db_connection


logger = logging.getLogger(__name__)

AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
DEFAULT_S3_BUCKET = os.getenv("S3_BUCKET")

AWS_CLIENT_CONFIG = Config(
    retries={
        "max_attempts": 5,
        "mode": "standard",
    }
)

s3_client = boto3.client(
    "s3",
    region_name=AWS_REGION,
    config=AWS_CLIENT_CONFIG,
)

glue_client = boto3.client(
    "glue",
    region_name=AWS_REGION,
    config=AWS_CLIENT_CONFIG,
)


class FileDeletionError(RuntimeError):
    """Raised when file resources cannot be safely deleted."""


def parse_s3_uri(s3_uri: str | None) -> tuple[str | None, str | None]:
    """
    Convert an S3 URI into bucket and key/prefix.

    Example:
        s3://my-bucket/lakehouse/iceberg/table/

    Returns:
        ("my-bucket", "lakehouse/iceberg/table/")
    """
    if not s3_uri:
        return None, None

    parsed = urlparse(s3_uri)

    if parsed.scheme != "s3" or not parsed.netloc:
        raise FileDeletionError(
            f"Invalid S3 URI stored for dataset: {s3_uri}"
        )

    bucket = parsed.netloc
    key = parsed.path.lstrip("/")

    return bucket, key


def table_exists(cursor, table_name: str) -> bool:
    """Check whether a PostgreSQL table exists in the public schema."""
    cursor.execute(
        """
        SELECT to_regclass(%s)
        """,
        (f"public.{table_name}",),
    )

    row = cursor.fetchone()
    return bool(row and row[0])


def get_file_deletion_metadata(
    *,
    user_id: int,
    file_name: str,
) -> dict[str, Any] | None:
    """
    Load all metadata required before removing external resources.

    AWS resource information must be read before PostgreSQL rows are deleted.
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

                    COALESCE(
                        fue.bucket_name,
                        %s
                    ) AS bucket_name,

                    sd.dataset_name,
                    sd.table_name,
                    sd.iceberg_database,
                    sd.iceberg_table,
                    sd.iceberg_s3_path

                FROM app_documents d

                LEFT JOIN structured_datasets sd
                  ON sd.document_id = d.id
                 AND sd.user_id = d.user_id

                LEFT JOIN file_upload_events fue
                  ON fue.document_id = d.id
                 AND fue.user_id = d.user_id

                WHERE d.user_id = %s
                  AND d.file_name = %s

                ORDER BY d.id DESC
                LIMIT 1
                """,
                (
                    DEFAULT_S3_BUCKET,
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
        raw_s3_key,
        bucket_name,
        dataset_name,
        table_name,
        iceberg_database,
        iceberg_table,
        iceberg_s3_path,
    ) = row

    return {
        "document_id": document_id,
        "file_name": stored_file_name,
        "file_type": file_type,
        "raw_s3_key": raw_s3_key,
        "bucket_name": bucket_name,
        "dataset_name": dataset_name,
        "table_name": table_name,
        "iceberg_database": iceberg_database,
        "iceberg_table": iceberg_table,
        "iceberg_s3_path": iceberg_s3_path,
    }


def get_glue_table_location(
    *,
    database_name: str | None,
    table_name: str | None,
) -> str | None:
    """
    Return the actual S3 location registered in Glue Data Catalog.

    Glue Catalog is preferred over the path stored in PostgreSQL because it
    represents the table location used by Athena.
    """
    if not database_name or not table_name:
        return None

    try:
        response = glue_client.get_table(
            DatabaseName=database_name,
            Name=table_name,
        )

        table = response.get("Table", {})
        storage_descriptor = table.get("StorageDescriptor", {})

        return storage_descriptor.get("Location")

    except glue_client.exceptions.EntityNotFoundException:
        logger.info(
            "Glue table already absent: %s.%s",
            database_name,
            table_name,
        )
        return None

    except ClientError as error:
        raise FileDeletionError(
            "Unable to read Glue Catalog table "
            f"{database_name}.{table_name}: {error}"
        ) from error


def delete_s3_object(
    *,
    bucket: str | None,
    key: str | None,
) -> bool:
    """Delete one S3 object, such as the original uploaded file."""
    if not bucket or not key:
        return False

    try:
        s3_client.delete_object(
            Bucket=bucket,
            Key=key,
        )

        logger.info(
            "Deleted raw S3 object: s3://%s/%s",
            bucket,
            key,
        )

        return True

    except ClientError as error:
        raise FileDeletionError(
            f"Unable to delete S3 object s3://{bucket}/{key}: {error}"
        ) from error


def delete_s3_prefix(
    *,
    bucket: str,
    prefix: str,
) -> int:
    """
    Delete every S3 object under a prefix.

    Iceberg tables contain many data, manifest, snapshot and metadata files.
    S3 prefixes are not real folders, so every matching object must be deleted.
    """
    normalized_prefix = prefix.lstrip("/")

    if not normalized_prefix:
        raise FileDeletionError(
            "Refusing to delete an empty S3 prefix"
        )

    paginator = s3_client.get_paginator("list_objects_v2")
    deleted_count = 0

    try:
        for page in paginator.paginate(
            Bucket=bucket,
            Prefix=normalized_prefix,
        ):
            objects = page.get("Contents", [])

            if not objects:
                continue

            # DeleteObjects accepts at most 1,000 keys per request.
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
                        "Some Iceberg objects could not be deleted: "
                        f"{errors[:5]}"
                    )

                deleted_count += len(batch)

    except ClientError as error:
        raise FileDeletionError(
            "Unable to delete Iceberg S3 prefix "
            f"s3://{bucket}/{normalized_prefix}: {error}"
        ) from error

    logger.info(
        "Deleted %s objects from Iceberg prefix s3://%s/%s",
        deleted_count,
        bucket,
        normalized_prefix,
    )

    return deleted_count


def delete_glue_table(
    *,
    database_name: str | None,
    table_name: str | None,
) -> bool:
    """Delete the Iceberg table entry from AWS Glue Data Catalog."""
    if not database_name or not table_name:
        return False

    try:
        glue_client.delete_table(
            DatabaseName=database_name,
            Name=table_name,
        )

        logger.info(
            "Deleted Glue Catalog table: %s.%s",
            database_name,
            table_name,
        )

        return True

    except glue_client.exceptions.EntityNotFoundException:
        logger.info(
            "Glue table already absent: %s.%s",
            database_name,
            table_name,
        )
        return False

    except ClientError as error:
        raise FileDeletionError(
            "Unable to delete Glue Catalog table "
            f"{database_name}.{table_name}: {error}"
        ) from error


def delete_optional_document_rows(
    *,
    cursor,
    table_name: str,
    user_id: int,
    document_id: int,
) -> int:
    """
    Delete document rows from an optional table.

    Some environments may not yet contain all tables because migrations were
    introduced at different stages of the project.
    """
    if not table_exists(cursor, table_name):
        logger.info(
            "Skipping missing PostgreSQL table: %s",
            table_name,
        )
        return 0

    # table_name is chosen only from fixed internal constants.
    cursor.execute(
        f"""
        DELETE FROM {table_name}
        WHERE user_id = %s
          AND document_id = %s
        """,
        (
            user_id,
            document_id,
        ),
    )

    return cur_rowcount(cursor)


def cur_rowcount(cursor) -> int:
    """Return a non-negative cursor row count."""
    return max(cursor.rowcount, 0)


def delete_database_records(
    *,
    user_id: int,
    document_id: int,
    file_name: str,
) -> dict[str, int]:
    """
    Remove all PostgreSQL records associated with one uploaded document.

    Child records are deleted before app_documents to respect foreign keys.
    """
    deleted: dict[str, int] = {}

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            deleted["query_cache"] = delete_optional_document_rows(
                cursor=cur,
                table_name="query_cache",
                user_id=user_id,
                document_id=document_id,
            )

            deleted["document_embeddings"] = delete_optional_document_rows(
                cursor=cur,
                table_name="document_embeddings",
                user_id=user_id,
                document_id=document_id,
            )

            deleted["mart_processed_chunks"] = (
                delete_optional_document_rows(
                    cursor=cur,
                    table_name="mart_processed_chunks",
                    user_id=user_id,
                    document_id=document_id,
                )
            )

            deleted["raw_chunks"] = delete_optional_document_rows(
                cursor=cur,
                table_name="raw_chunks",
                user_id=user_id,
                document_id=document_id,
            )

            if table_exists(cur, "chat_history"):
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
                deleted["chat_history"] = cur_rowcount(cur)
            else:
                deleted["chat_history"] = 0

            if table_exists(cur, "structured_datasets"):
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
                deleted["structured_datasets"] = cur_rowcount(cur)
            else:
                deleted["structured_datasets"] = 0

            if table_exists(cur, "file_upload_events"):
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
                deleted["file_upload_events"] = cur_rowcount(cur)
            else:
                deleted["file_upload_events"] = 0

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
            deleted["app_documents"] = cur_rowcount(cur)

    return deleted


def resolve_iceberg_location(
    metadata: dict[str, Any],
) -> str | None:
    """
    Determine the Iceberg table location.

    Prefer Glue Catalog, then fall back to PostgreSQL metadata.
    """
    glue_location = get_glue_table_location(
        database_name=metadata.get("iceberg_database"),
        table_name=metadata.get("iceberg_table"),
    )

    return glue_location or metadata.get("iceberg_s3_path")


def delete_structured_resources(
    metadata: dict[str, Any],
) -> dict[str, Any]:
    """
    Delete Iceberg storage and Glue Catalog metadata.

    Order:
        1. Resolve the real table location.
        2. Delete Iceberg objects from S3.
        3. Delete the Glue Catalog entry.

    The location is resolved before deleting the catalog entry so the table
    path is not lost.
    """
    deleted = {
        "iceberg_s3_objects": 0,
        "glue_catalog_table": False,
    }

    iceberg_location = resolve_iceberg_location(metadata)

    if iceberg_location:
        iceberg_bucket, iceberg_prefix = parse_s3_uri(
            iceberg_location
        )

        if iceberg_bucket and iceberg_prefix:
            deleted["iceberg_s3_objects"] = delete_s3_prefix(
                bucket=iceberg_bucket,
                prefix=iceberg_prefix,
            )
    else:
        logger.warning(
            "No Iceberg S3 location found for document %s",
            metadata["document_id"],
        )

    deleted["glue_catalog_table"] = delete_glue_table(
        database_name=metadata.get("iceberg_database"),
        table_name=metadata.get("iceberg_table"),
    )

    return deleted


def delete_user_file(
    user_id: int,
    file_name: str,
) -> dict[str, Any] | None:
    """
    Delete one user-owned uploaded file and all associated resources.

    Structured flow:
        Iceberg S3 files
        -> Glue Catalog table
        -> raw upload
        -> PostgreSQL metadata

    Unstructured flow:
        raw upload
        -> chunks and embeddings
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
    bucket_name = metadata["bucket_name"]
    raw_s3_key = metadata["raw_s3_key"]

    deleted: dict[str, Any] = {
        "raw_s3_object": False,
        "iceberg_s3_objects": 0,
        "glue_catalog_table": False,
    }

    external_errors: list[str] = []

    if file_type == "structured":
        try:
            structured_result = delete_structured_resources(
                metadata
            )
            deleted.update(structured_result)

        except Exception as error:
            logger.exception(
                "Structured AWS cleanup failed for document %s",
                document_id,
            )
            external_errors.append(str(error))

    try:
        deleted["raw_s3_object"] = delete_s3_object(
            bucket=bucket_name,
            key=raw_s3_key,
        )

    except Exception as error:
        logger.exception(
            "Raw S3 deletion failed for document %s",
            document_id,
        )
        external_errors.append(str(error))

    # Keep PostgreSQL metadata when external cleanup fails.
    # This allows deletion to be retried and prevents losing the paths needed
    # to locate remaining AWS resources.
    if external_errors:
        return {
            "status": "partial_failed",
            "message": (
                "Some AWS resources could not be removed. "
                "PostgreSQL metadata was kept so deletion can be retried."
            ),
            "user_id": user_id,
            "document_id": document_id,
            "file": file_name,
            "file_type": file_type,
            "deleted": deleted,
            "errors": external_errors,
        }

    try:
        deleted["database"] = delete_database_records(
            user_id=user_id,
            document_id=document_id,
            file_name=file_name,
        )

    except Exception as error:
        logger.exception(
            "PostgreSQL cleanup failed for document %s",
            document_id,
        )

        return {
            "status": "partial_failed",
            "message": (
                "AWS resources were removed, but PostgreSQL cleanup failed."
            ),
            "user_id": user_id,
            "document_id": document_id,
            "file": file_name,
            "file_type": file_type,
            "deleted": deleted,
            "errors": [str(error)],
        }

    logger.info(
        "File deletion completed: user=%s document=%s file=%s type=%s",
        user_id,
        document_id,
        file_name,
        file_type,
    )

    return {
        "status": "deleted",
        "message": "File and related resources deleted successfully.",
        "user_id": user_id,
        "document_id": document_id,
        "file": file_name,
        "file_type": file_type,
        "deleted": deleted,
    }