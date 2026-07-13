import os
from pydantic import BaseModel
from fastapi import APIRouter, Depends, Header, HTTPException

from app.db import get_db_connection
from app.auth import get_current_user
from app.services.dbt_service import run_dbt_build
from app.services.cloudwatch_metrics import send_metric

router = APIRouter()

INTERNAL_API_KEY = os.getenv("INTERNAL_API_KEY", "")


@router.post("/dbt/run")
def run_dbt(current_user: dict = Depends(get_current_user)):
    try:
        result = run_dbt_build()
        send_metric("DBTRunSuccess", 1)
        return {
            "user_id": current_user["id"],
            "dbt_status": result
        }
    except Exception:
        send_metric("DBTRunFailed", 1)
        raise


@router.post("/internal/dbt/run")
def internal_run_dbt(x_internal_api_key: str = Header(None)):
    if not INTERNAL_API_KEY:
        raise HTTPException(status_code=500, detail="INTERNAL_API_KEY not set")

    if x_internal_api_key != INTERNAL_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid internal API key")

    try:
        result = run_dbt_build()
        send_metric("DBTRunSuccess", 1)
        return {"dbt_status": result}
    except Exception:
        send_metric("DBTRunFailed", 1)
        raise
    
class DocumentErrorRequest(BaseModel):
    user_id: int
    document_id: int
    file_type: str | None = None
    error: str = "Processing failed"


@router.post("/internal/documents/mark-error")
def mark_document_error(
    req: DocumentErrorRequest,
    x_internal_api_key: str = Header(None)
):
    if not INTERNAL_API_KEY:
        raise HTTPException(status_code=500, detail="INTERNAL_API_KEY not set")

    if x_internal_api_key != INTERNAL_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid internal API key")

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE app_documents
                SET processing_status = 'error',
                    processing_error = %s,
                    updated_at = NOW()
                WHERE user_id = %s
                  AND id = %s
            """, (req.error, req.user_id, req.document_id))

            cur.execute("""
                UPDATE file_upload_events
                SET status = 'error'
                WHERE user_id = %s
                  AND document_id = %s
            """, (req.user_id, req.document_id))

            if req.file_type == "structured":
                cur.execute("""
                    UPDATE structured_datasets
                    SET status = 'error',
                        updated_at = NOW()
                    WHERE user_id = %s
                      AND document_id = %s
                """, (req.user_id, req.document_id))

    send_metric("FilesFailed", 1)

    return {
        "status": "error_updated",
        "user_id": req.user_id,
        "document_id": req.document_id,
    }