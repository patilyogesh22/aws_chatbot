import os
import io
import hashlib

from fastapi import APIRouter, File, UploadFile, HTTPException, Depends

from app.db import get_db_connection
from app.auth import get_current_user
from app.utils.file_classifier import classify_file
from app.utils.structured_converter import convert_excel_to_csv
from app.services.queue_service import send_file_to_queue, clean_table_name
from aws.s3_ingestion import upload_fileobj_to_s3


router = APIRouter()

SUPPORTED_MESSAGE = "Unsupported file type. Supported: CSV, Excel, JSON, PDF, DOCX, TXT, MD, PPTX"


def update_processing_status(
    *,
    user_id: int,
    document_id: int,
    document_status: str | None = None,
    structured_status: str | None = None,
    event_status: str | None = None,
    error: str | None = None,
):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            if document_status:
                cur.execute("""
                    UPDATE app_documents
                    SET processing_status = %s,
                        processing_error = %s,
                        updated_at = NOW()
                    WHERE user_id = %s
                      AND id = %s
                """, (
                    document_status,
                    error,
                    user_id,
                    document_id,
                ))

            if structured_status:
                cur.execute("""
                    UPDATE structured_datasets
                    SET status = %s,
                        updated_at = NOW()
                    WHERE user_id = %s
                      AND document_id = %s
                """, (
                    structured_status,
                    user_id,
                    document_id,
                ))

            if event_status:
                cur.execute("""
                    UPDATE file_upload_events
                    SET status = %s,
                        updated_at = NOW()
                    WHERE user_id = %s
                      AND document_id = %s
                """, (
                    event_status,
                    user_id,
                    document_id,
                ))


def insert_file_upload_event(
    *,
    user_id: int,
    file_name: str,
    s3_key: str,
    file_size: int,
    file_type: str,
    document_id: int,
    status: str,
    bucket_name: str,
    dataset_name: str,
    table_name: str,
):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
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
                    table_name,
                    status,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            """, (
                user_id,
                file_name,
                s3_key,
                bucket_name,
                file_size,
                file_type,
                document_id,
                dataset_name,
                table_name,
                status,
            ))


def get_duplicate_file(*, user_id: int, file_hash: str):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    d.id,
                    d.file_name,
                    d.file_type,
                    d.s3_key,
                    COALESCE(d.processing_status, '') AS document_status,
                    COALESCE(sd.status, '') AS structured_status
                FROM app_documents d
                LEFT JOIN structured_datasets sd
                  ON sd.document_id = d.id
                 AND sd.user_id = d.user_id
                WHERE d.user_id = %s
                  AND d.file_hash = %s
                ORDER BY d.id DESC
                LIMIT 1
            """, (
                user_id,
                file_hash,
            ))

            return cur.fetchone()


def duplicate_response(duplicate):
    existing_document_id = duplicate[0]
    existing_file_name = duplicate[1]
    existing_file_type = duplicate[2]
    existing_s3_key = duplicate[3]
    existing_document_status = duplicate[4]
    existing_structured_status = duplicate[5]

    current_status = existing_structured_status or existing_document_status

    retry_statuses = {
        "upload_saved",
        "sqs_failed",
        "uploaded",
        "glue_job_pending",
    }

    if current_status in retry_statuses:
        return {
            "status": "retry_required",
            "message": "This file already exists but was not queued successfully. Use retry_queue endpoint instead of uploading again.",
            "document_id": existing_document_id,
            "file": existing_file_name,
            "file_type": existing_file_type,
            "s3_key": existing_s3_key,
            "current_processing_status": current_status,
            "retry_endpoint": f"/upload/{existing_document_id}/retry-queue",
        }

    return {
        "status": "duplicate",
        "message": "Duplicate file already uploaded by this user",
        "document_id": existing_document_id,
        "file": existing_file_name,
        "file_type": existing_file_type,
        "s3_key": existing_s3_key,
        "current_processing_status": current_status,
    }


def _process_upload_content(
    *,
    content: bytes,
    filename: str,
    user_id: int,
    raise_on_duplicate: bool = True,
):
    file_size = len(content)

    if file_size == 0:
        if raise_on_duplicate:
            raise HTTPException(status_code=400, detail="Uploaded file is empty")
        return {
            "status": "error",
            "file": filename,
            "message": "Uploaded file is empty",
        }

    file_hash = hashlib.md5(content).hexdigest()
    file_type = classify_file(filename)

    if file_type == "unknown":
        if raise_on_duplicate:
            raise HTTPException(status_code=400, detail=SUPPORTED_MESSAGE)
        return {
            "status": "error",
            "file": filename,
            "message": SUPPORTED_MESSAGE,
        }

    s3_bucket = os.getenv("S3_BUCKET")
    if not s3_bucket:
        raise HTTPException(status_code=500, detail="S3_BUCKET not set")

    duplicate = get_duplicate_file(user_id=user_id, file_hash=file_hash)
    if duplicate:
        dup_response = duplicate_response(duplicate)

        if raise_on_duplicate and dup_response["status"] == "duplicate":
            raise HTTPException(
                status_code=400,
                detail={
                    "message": dup_response["message"],
                    "document_id": dup_response["document_id"],
                    "file": dup_response["file"],
                    "file_type": dup_response["file_type"],
                    "current_processing_status": dup_response["current_processing_status"],
                }
            )

        return dup_response

    s3_prefix = f"uploads/user_{user_id}/{file_type}/"

    upload_obj = io.BytesIO(content)
    upload_filename = filename
    temp_csv_path = None

    if file_type == "structured":
        ext = os.path.splitext(filename)[1].lower()

        if ext in [".xlsx", ".xls"]:
            temp_csv_path, upload_filename = convert_excel_to_csv(
                io.BytesIO(content),
                filename,
            )
            upload_obj = open(temp_csv_path, "rb")

    try:
        s3_result = upload_fileobj_to_s3(
            file_obj=upload_obj,
            filename=upload_filename,
            prefix=s3_prefix,
        )
    finally:
        try:
            upload_obj.close()
        except Exception:
            pass

        if temp_csv_path:
            os.remove(temp_csv_path)

    s3_key = s3_result["s3_key"]
    original_filename = s3_result["original_filename"]

    file_type = classify_file(original_filename)
    if file_type == "unknown":
        raise HTTPException(status_code=400, detail="Unsupported file type after upload")

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO app_documents
                (
                    user_id,
                    file_name,
                    file_hash,
                    file_type,
                    s3_key,
                    file_size,
                    processing_status
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                user_id,
                original_filename,
                file_hash,
                file_type,
                s3_key,
                file_size,
                "upload_saved",
            ))

            document_id = cur.fetchone()[0]

    dataset_name = clean_table_name(original_filename)
    table_name = f"u{user_id}_d{document_id}_{dataset_name}"

    insert_file_upload_event(
        user_id=user_id,
        file_name=original_filename,
        s3_key=s3_key,
        file_size=file_size,
        file_type=file_type,
        document_id=document_id,
        status="upload_saved",
        bucket_name=s3_bucket,
        dataset_name=dataset_name,
        table_name=table_name,
    )

    if file_type == "structured":
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO structured_datasets
                    (
                        user_id,
                        document_id,
                        file_name,
                        dataset_name,
                        table_name,
                        raw_s3_key,
                        status
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (user_id, document_id)
                    DO UPDATE SET
                        file_name = EXCLUDED.file_name,
                        dataset_name = EXCLUDED.dataset_name,
                        table_name = EXCLUDED.table_name,
                        raw_s3_key = EXCLUDED.raw_s3_key,
                        status = EXCLUDED.status,
                        updated_at = NOW()
                """, (
                    user_id,
                    document_id,
                    original_filename,
                    dataset_name,
                    table_name,
                    s3_key,
                    "upload_saved",
                ))

    try:
        sqs_message_id, dataset_name, table_name = send_file_to_queue(
            user_id=user_id,
            document_id=document_id,
            file_name=original_filename,
            file_type=file_type,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            file_size=file_size,
        )

        if file_type == "structured":
            update_processing_status(
                user_id=user_id,
                document_id=document_id,
                document_status="sqs_queued",
                structured_status="sqs_queued",
                event_status="sqs_queued",
            )

            next_step = "SQS → EventBridge Pipe → Step Functions → Glue Job → RDS table → NL-to-SQL"
            message = "Structured file uploaded successfully and sent to SQS for processing."
        else:
            update_processing_status(
                user_id=user_id,
                document_id=document_id,
                document_status="sqs_queued",
                event_status="sqs_queued",
            )

            next_step = "SQS → EventBridge Pipe → Step Functions → ECS Fargate → chunks + embeddings"
            message = "Unstructured file uploaded successfully and sent to SQS for ECS background processing."

        return {
            "status": "success",
            "file_type": file_type,
            "message": message,
            "file": original_filename,
            "s3_key": s3_key,
            "file_size": file_size,
            "document_id": document_id,
            "sqs_message_id": sqs_message_id,
            "processing_status": "sqs_queued",
            "dataset_name": dataset_name,
            "table_name": table_name,
            "next_step": next_step,
        }

    except Exception as sqs_error:
        if file_type == "structured":
            update_processing_status(
                user_id=user_id,
                document_id=document_id,
                document_status="sqs_failed",
                structured_status="sqs_failed",
                event_status="sqs_failed",
                error=str(sqs_error),
            )
        else:
            update_processing_status(
                user_id=user_id,
                document_id=document_id,
                document_status="sqs_failed",
                event_status="sqs_failed",
                error=str(sqs_error),
            )

        if raise_on_duplicate:
            raise HTTPException(
                status_code=500,
                detail={
                    "message": "File uploaded and metadata saved, but sending to SQS failed.",
                    "document_id": document_id,
                    "file": original_filename,
                    "s3_key": s3_key,
                    "reason": str(sqs_error),
                    "next_action": f"Fix SQS and call /upload/{document_id}/retry-queue",
                }
            )

        return {
            "status": "sqs_failed",
            "file": original_filename,
            "file_type": file_type,
            "document_id": document_id,
            "s3_key": s3_key,
            "message": "File uploaded and metadata saved, but sending to SQS failed.",
            "reason": str(sqs_error),
            "retry_endpoint": f"/upload/{document_id}/retry-queue",
        }


@router.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    current_user: dict = Depends(get_current_user),
):
    try:
        content = await file.read()
        return _process_upload_content(
            content=content,
            filename=file.filename,
            user_id=current_user["id"],
            raise_on_duplicate=True,
        )

    except HTTPException:
        raise

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/upload/batch")
async def upload_files_batch(
    files: list[UploadFile] = File(...),
    current_user: dict = Depends(get_current_user),
):
    if not files:
        raise HTTPException(status_code=400, detail="No files uploaded")

    if len(files) > 20:
        raise HTTPException(status_code=400, detail="Maximum 20 files can be uploaded at once")

    results = []

    for file in files:
        try:
            content = await file.read()
            result = _process_upload_content(
                content=content,
                filename=file.filename,
                user_id=current_user["id"],
                raise_on_duplicate=False,
            )
            results.append(result)

        except Exception as e:
            results.append({
                "status": "error",
                "file": file.filename,
                "message": str(e),
            })

    success_count = sum(1 for r in results if r.get("status") == "success")
    duplicate_count = sum(1 for r in results if r.get("status") == "duplicate")
    retry_required_count = sum(1 for r in results if r.get("status") == "retry_required")
    error_count = sum(1 for r in results if r.get("status") in {"error", "sqs_failed"})

    return {
        "status": "completed",
        "total_files": len(files),
        "success_count": success_count,
        "duplicate_count": duplicate_count,
        "retry_required_count": retry_required_count,
        "error_count": error_count,
        "results": results,
    }


@router.post("/upload/{document_id}/retry-queue")
async def retry_queue_document(
    document_id: int,
    current_user: dict = Depends(get_current_user),
):
    try:
        s3_bucket = os.getenv("S3_BUCKET")
        if not s3_bucket:
            raise HTTPException(status_code=500, detail="S3_BUCKET not set")

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        d.id,
                        d.user_id,
                        d.file_name,
                        d.file_type,
                        d.s3_key,
                        d.file_size,
                        COALESCE(d.processing_status, '') AS document_status,
                        COALESCE(sd.status, '') AS structured_status
                    FROM app_documents d
                    LEFT JOIN structured_datasets sd
                      ON sd.document_id = d.id
                     AND sd.user_id = d.user_id
                    WHERE d.id = %s
                      AND d.user_id = %s
                    LIMIT 1
                """, (
                    document_id,
                    current_user["id"],
                ))

                row = cur.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Document not found")

        _, user_id, file_name, file_type, s3_key, file_size, old_doc_status, old_structured_status = row

        if file_type not in {"structured", "unstructured"}:
            raise HTTPException(
                status_code=400,
                detail="Retry queue is only for structured and unstructured files currently"
            )

        sqs_message_id, dataset_name, table_name = send_file_to_queue(
            user_id=user_id,
            document_id=document_id,
            file_name=file_name,
            file_type=file_type,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            file_size=file_size or 0,
        )

        if file_type == "structured":
            update_processing_status(
                user_id=current_user["id"],
                document_id=document_id,
                document_status="sqs_queued",
                structured_status="sqs_queued",
                event_status="sqs_queued",
            )
        else:
            update_processing_status(
                user_id=current_user["id"],
                document_id=document_id,
                document_status="sqs_queued",
                event_status="sqs_queued",
            )

        return {
            "status": "success",
            "message": "Document sent to SQS again.",
            "document_id": document_id,
            "file": file_name,
            "file_type": file_type,
            "old_status": old_structured_status or old_doc_status,
            "new_status": "sqs_queued",
            "sqs_message_id": sqs_message_id,
            "dataset_name": dataset_name,
            "table_name": table_name,
        }

    except HTTPException:
        raise

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))