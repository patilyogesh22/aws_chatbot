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
                        d.file_name,
                        COALESCE(d.file_type, 'unknown') AS file_type,
                        d.file_size,
                        d.uploaded_at,
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
                        d.uploaded_at
                    ORDER BY d.uploaded_at DESC
                """, (current_user["id"],))

                rows = cur.fetchall()

        return {
            "files": [
                {
                    "name": r[0],
                    "file_type": r[1],
                    "size": r[2] or 0,
                    "uploaded_at": r[3].isoformat() if r[3] else None,
                    "chunks": r[4] or 0,
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
                SELECT status, table_name, row_count, glue_job_run_id, updated_at
                FROM structured_datasets
                WHERE user_id = %s
                  AND file_name = %s
                ORDER BY id DESC
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

    status, table_name, row_count, run_id, updated_at = row

    messages = {
        "glue_job_pending": "File uploaded. Waiting for AWS Glue to start…",
        "glue_job_started": "AWS Glue is processing your file (1–3 min)…",
        "ready": "File is ready. You can ask questions now.",
        "error": "Processing failed. Please re-upload the file.",
    }

    return {
        "file_name": file_name,
        "status": status,
        "ready": status == "ready",
        "table_name": table_name,
        "row_count": row_count,
        "glue_job_run_id": run_id,
        "updated_at": updated_at.isoformat() if updated_at else None,
        "message": messages.get(status, f"Status: {status}"),
    }