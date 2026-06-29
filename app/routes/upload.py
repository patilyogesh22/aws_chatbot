import os
import hashlib
import psycopg2

from fastapi import APIRouter, File, UploadFile, HTTPException, Depends

from app.config import PG_DSN
from app.auth import get_current_user
from app.utils.file_classifier import classify_file
from app.utils.structured_converter import convert_excel_to_csv
from app.services.queue_service import send_file_to_queue
from aws.s3_ingestion import upload_fileobj_to_s3


router = APIRouter()


def update_processing_status(
    *,
    user_id: int,
    document_id: int,
    document_status: str | None = None,
    structured_status: str | None = None,
    event_status: str | None = None,
    error: str | None = None,
):
    with psycopg2.connect(PG_DSN) as conn:
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
                    SET status = %s
                    WHERE user_id = %s
                      AND document_id = %s
                """, (
                    event_status,
                    user_id,
                    document_id,
                ))

        conn.commit()


def insert_file_upload_event(
    *,
    user_id: int,
    file_name: str,
    s3_key: str,
    file_size: int,
    file_type: str,
    document_id: int,
    status: str,
):
    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO file_upload_events
                (
                    user_id,
                    file_name,
                    s3_key,
                    file_size,
                    file_type,
                    document_id,
                    status
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                user_id,
                file_name,
                s3_key,
                file_size,
                file_type,
                document_id,
                status,
            ))
        conn.commit()


@router.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    current_user: dict = Depends(get_current_user)
):
    try:
        content = await file.read()
        file_size = len(content)
        file_hash = hashlib.md5(content).hexdigest()

        if file_size == 0:
            raise HTTPException(status_code=400, detail="Uploaded file is empty")

        file_type = classify_file(file.filename)

        if file_type == "unknown":
            raise HTTPException(
                status_code=400,
                detail="Unsupported file type. Supported: CSV, Excel, JSON, PDF, DOCX, TXT, MD, PPTX"
            )

        file.file.seek(0)

        s3_bucket = os.getenv("S3_BUCKET")
        if not s3_bucket:
            raise HTTPException(status_code=500, detail="S3_BUCKET not set")

        # Duplicate file check
        with psycopg2.connect(PG_DSN) as conn:
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
                    current_user["id"],
                    file_hash,
                ))

                duplicate = cur.fetchone()

        if duplicate:
            existing_document_id = duplicate[0]
            existing_file_name = duplicate[1]
            existing_file_type = duplicate[2]
            existing_s3_key = duplicate[3]
            existing_document_status = duplicate[4]
            existing_structured_status = duplicate[5]

            retry_statuses = {
                "upload_saved",
                "sqs_failed",
                "uploaded",
                "glue_job_pending",
            }

            current_status = existing_structured_status or existing_document_status

            if current_status in retry_statuses:
                return {
                    "status": "retry_required",
                    "message": "This file already exists but was not queued successfully. Use retry_queue endpoint instead of uploading again.",
                    "document_id": existing_document_id,
                    "file": existing_file_name,
                    "file_type": existing_file_type,
                    "s3_key": existing_s3_key,
                    "current_processing_status": current_status,
                    "retry_endpoint": f"/upload/{existing_document_id}/retry-queue"
                }

            raise HTTPException(
                status_code=400,
                detail={
                    "message": "Duplicate file already uploaded by this user",
                    "document_id": existing_document_id,
                    "file": existing_file_name,
                    "file_type": existing_file_type,
                    "current_processing_status": current_status,
                }
            )

        s3_prefix = f"uploads/user_{current_user['id']}/{file_type}/"

        upload_obj = file.file
        upload_filename = file.filename
        temp_csv_path = None

        if file_type == "structured":
            ext = os.path.splitext(file.filename)[1].lower()

            if ext in [".xlsx", ".xls"]:
                temp_csv_path, upload_filename = convert_excel_to_csv(
                    file.file,
                    file.filename
                )
                upload_obj = open(temp_csv_path, "rb")

        try:
            s3_result = upload_fileobj_to_s3(
                file_obj=upload_obj,
                filename=upload_filename,
                prefix=s3_prefix
            )
        finally:
            if upload_obj is not file.file:
                upload_obj.close()

            if temp_csv_path:
                os.remove(temp_csv_path)

        s3_key = s3_result["s3_key"]
        original_filename = s3_result["original_filename"]

        file_type = classify_file(original_filename)

        if file_type == "unknown":
            raise HTTPException(status_code=400, detail="Unsupported file type after upload")

        # Insert document metadata with correct initial status
        with psycopg2.connect(PG_DSN) as conn:
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
                    current_user["id"],
                    original_filename,
                    file_hash,
                    file_type,
                    s3_key,
                    file_size,
                    "upload_saved",
                ))

                document_id = cur.fetchone()[0]

            conn.commit()

        # Common event for both structured and unstructured files
        insert_file_upload_event(
            user_id=current_user["id"],
            file_name=original_filename,
            s3_key=s3_key,
            file_size=file_size,
            file_type=file_type,
            document_id=document_id,
            status="upload_saved",
        )

        # Structured metadata
        if file_type == "structured":
            with psycopg2.connect(PG_DSN) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO structured_datasets
                        (
                            user_id,
                            document_id,
                            file_name,
                            raw_s3_key,
                            status
                        )
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (user_id, document_id)
                        DO UPDATE SET
                            file_name = EXCLUDED.file_name,
                            raw_s3_key = EXCLUDED.raw_s3_key,
                            status = EXCLUDED.status,
                            updated_at = NOW()
                    """, (
                        current_user["id"],
                        document_id,
                        original_filename,
                        s3_key,
                        "upload_saved"
                    ))

                conn.commit()

        # Queue both structured and unstructured files
        try:
            sqs_message_id = send_file_to_queue(
                user_id=current_user["id"],
                document_id=document_id,
                file_name=original_filename,
                file_type=file_type,
                s3_bucket=s3_bucket,
                s3_key=s3_key,
                file_size=file_size,
            )

            if file_type == "structured":
                update_processing_status(
                    user_id=current_user["id"],
                    document_id=document_id,
                    document_status="sqs_queued",
                    structured_status="sqs_queued",
                    event_status="sqs_queued",
                )

                next_step = "SQS → Lambda → Step Functions → Glue Job → RDS table → NL-to-SQL"
                message = "Structured file uploaded successfully and sent to SQS for processing."

            else:
                update_processing_status(
                    user_id=current_user["id"],
                    document_id=document_id,
                    document_status="sqs_queued",
                    event_status="sqs_queued",
                )

                next_step = "SQS → Lambda → Step Functions → ECS Fargate → chunks + embeddings"
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
                "next_step": next_step,
            }

        except Exception as sqs_error:
            if file_type == "structured":
                update_processing_status(
                    user_id=current_user["id"],
                    document_id=document_id,
                    document_status="sqs_failed",
                    structured_status="sqs_failed",
                    event_status="sqs_failed",
                    error=str(sqs_error),
                )
            else:
                update_processing_status(
                    user_id=current_user["id"],
                    document_id=document_id,
                    document_status="sqs_failed",
                    event_status="sqs_failed",
                    error=str(sqs_error),
                )

            raise HTTPException(
                status_code=500,
                detail={
                    "message": "File uploaded and metadata saved, but sending to SQS failed.",
                    "document_id": document_id,
                    "file": original_filename,
                    "s3_key": s3_key,
                    "reason": str(sqs_error),
                    "next_action": f"Fix SQS and call /upload/{document_id}/retry-queue"
                }
            )

    except HTTPException:
        raise

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/upload/{document_id}/retry-queue")
async def retry_queue_document(
    document_id: int,
    current_user: dict = Depends(get_current_user)
):
    try:
        s3_bucket = os.getenv("S3_BUCKET")
        if not s3_bucket:
            raise HTTPException(status_code=500, detail="S3_BUCKET not set")

        with psycopg2.connect(PG_DSN) as conn:
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
                    current_user["id"]
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

        sqs_message_id = send_file_to_queue(
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
            "sqs_message_id": sqs_message_id
        }

    except HTTPException:
        raise

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))