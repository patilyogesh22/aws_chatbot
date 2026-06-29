import psycopg2

from fastapi import APIRouter, Depends, HTTPException

from app.config import PG_DSN
from app.auth import get_current_user
from app.services.file_delete_service import delete_user_file

router = APIRouter()


@router.get("/files")
def list_files(current_user: dict = Depends(get_current_user)):
    try:
        with psycopg2.connect(PG_DSN) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        d.id,
                        d.file_name,
                        COALESCE(d.file_type, 'unknown') AS file_type,
                        d.file_size,
                        d.uploaded_at,
                        COALESCE(d.processing_status, 'upload_saved') AS processing_status,
                        d.processing_error,
                        COALESCE(COUNT(r.chunk_id), 0) AS chunk_count
                    FROM app_documents d
                    LEFT JOIN raw_chunks r
                        ON r.document_id = d.id
                       AND r.user_id = d.user_id
                    WHERE d.user_id = %s
                    GROUP BY
                        d.id,
                        d.file_name,
                        d.file_type,
                        d.file_size,
                        d.uploaded_at,
                        d.processing_status,
                        d.processing_error
                    ORDER BY d.uploaded_at DESC
                """, (current_user["id"],))

                rows = cur.fetchall()

        return {
            "files": [
                {
                    "document_id": r[0],
                    "name": r[1],
                    "file_type": r[2],
                    "size": r[3] or 0,
                    "uploaded_at": r[4].isoformat() if r[4] else None,
                    "processing_status": r[5],
                    "processing_error": r[6],
                    "chunks": r[7] or 0,
                    "ready": r[5] == "ready",
                }
                for r in rows
            ]
        }

    except Exception as e:
        return {
            "files": [],
            "error": str(e)
        }


@router.delete("/files/{file_name}")
def delete_file(
    file_name: str,
    current_user: dict = Depends(get_current_user)
):
    result = delete_user_file(
        user_id=current_user["id"],
        file_name=file_name
    )

    if not result:
        raise HTTPException(status_code=404, detail="File not found")

    return result


@router.get("/structured/status/{file_name}")
def structured_status(
    file_name: str,
    current_user: dict = Depends(get_current_user)
):
    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    d.file_type,
                    COALESCE(d.processing_status, 'upload_saved') AS processing_status,
                    d.processing_error,
                    COALESCE(sd.status, '') AS structured_status,
                    sd.table_name,
                    sd.row_count,
                    sd.glue_job_run_id,
                    COALESCE(sd.updated_at, d.updated_at, d.uploaded_at) AS updated_at
                FROM app_documents d
                LEFT JOIN structured_datasets sd
                  ON sd.document_id = d.id
                 AND sd.user_id = d.user_id
                WHERE d.user_id = %s
                  AND d.file_name = %s
                ORDER BY d.id DESC
                LIMIT 1
            """, (current_user["id"], file_name))

            row = cur.fetchone()

    if not row:
        return {
            "file_name": file_name,
            "status": "not_found",
            "ready": False,
            "message": "File not found. Please upload it first.",
        }

    (
        file_type,
        processing_status,
        processing_error,
        structured_status,
        table_name,
        row_count,
        run_id,
        updated_at,
    ) = row

    status = structured_status if file_type == "structured" and structured_status else processing_status

    messages = {
        "upload_saved": "File uploaded. Waiting to be queued…",
        "sqs_queued": "File queued for background processing…",
        "step_function_started": "Workflow started. Processing will begin shortly…",
        "processing": "File is being processed…",
        "glue_job_pending": "File uploaded. Waiting for AWS Glue to start…",
        "glue_job_started": "AWS Glue is processing your file…",
        "ready": "File is ready. You can ask questions now.",
        "error": "Processing failed. Please re-upload the file.",
        "sqs_failed": "Failed to send file to processing queue.",
        "not_found": "File not found. Please upload it first.",
    }

    return {
        "file_name": file_name,
        "file_type": file_type,
        "status": status,
        "ready": status == "ready",
        "table_name": table_name,
        "row_count": row_count,
        "glue_job_run_id": run_id,
        "processing_error": processing_error,
        "updated_at": updated_at.isoformat() if updated_at else None,
        "message": processing_error or messages.get(status, f"Status: {status}"),
    }