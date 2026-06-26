import os
import hashlib
import psycopg2

from fastapi import APIRouter, File, UploadFile, HTTPException, Depends

from app.config import PG_DSN
from app.auth import get_current_user
from app.utils.file_classifier import classify_file
from app.utils.structured_converter import convert_excel_to_csv
from app.services.ingestion_service import ingest_file_from_s3_key
from app.services.embedding_service import embed_from_postgres
from app.services.dbt_service import run_dbt_build
from app.services.queue_service import send_file_to_queue
from aws.s3_ingestion import upload_fileobj_to_s3


router = APIRouter()


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
                    SELECT id
                    FROM app_documents
                    WHERE user_id = %s
                      AND file_hash = %s
                    LIMIT 1
                """, (current_user["id"], file_hash))

                if cur.fetchone():
                    raise HTTPException(
                        status_code=400,
                        detail="Duplicate file already uploaded by this user"
                    )

        s3_prefix = f"uploads/user_{current_user['id']}/{file_type}/"

        upload_obj = file.file
        upload_filename = file.filename
        temp_csv_path = None

        # Convert Excel to CSV before uploading to S3
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

        # Insert document metadata
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
                        file_size
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    current_user["id"],
                    original_filename,
                    file_hash,
                    file_type,
                    s3_key,
                    file_size
                ))

                document_id = cur.fetchone()[0]

            conn.commit()

        # Structured file flow:
        # FastAPI → S3 → PostgreSQL metadata → SQS → Lambda → Glue
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
                        "sqs_queued"
                    ))

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
                        current_user["id"],
                        original_filename,
                        s3_key,
                        file_size,
                        "structured",
                        document_id,
                        "sqs_queued"
                    ))

                conn.commit()

            # Send message to SQS after DB metadata is saved
            sqs_message_id = send_file_to_queue(
                user_id=current_user["id"],
                document_id=document_id,
                file_name=original_filename,
                file_type=file_type,
                s3_bucket=s3_bucket,
                s3_key=s3_key,
                file_size=file_size,
            )

            return {
                "status": "success",
                "file_type": "structured",
                "message": "Structured file uploaded successfully and sent to SQS for processing.",
                "file": original_filename,
                "s3_key": s3_key,
                "file_size": file_size,
                "document_id": document_id,
                "sqs_message_id": sqs_message_id,
                "next_step": "SQS → Lambda → Glue Job → isolated RDS table → NL-to-SQL"
            }

        # Unstructured file flow:
        # Keep current backend processing until ECS Fargate phase
        chunks = ingest_file_from_s3_key(
            bucket=s3_bucket,
            s3_key=s3_key,
            file_name=original_filename,
            file_hash=file_hash,
            user_id=current_user["id"],
            document_id=document_id
        )

        dbt_status = run_dbt_build()

        embedded_count = embed_from_postgres(
            user_id=current_user["id"],
            file_name=original_filename
        )

        return {
            "status": "success",
            "file_type": "unstructured",
            "message": "Unstructured file processed and embedded successfully.",
            "file": original_filename,
            "file_size": file_size,
            "document_id": document_id,
            "chunks": len(chunks),
            "embedded": embedded_count,
            "dbt_status": dbt_status,
        }

    except HTTPException:
        raise

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))