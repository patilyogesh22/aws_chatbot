import os
import psycopg2
from pydantic import BaseModel
from fastapi import APIRouter, Depends, Header, HTTPException

from app.config import PG_DSN
from app.auth import get_current_user
from app.services.dbt_service import run_dbt_build

router = APIRouter()

INTERNAL_API_KEY = os.getenv("INTERNAL_API_KEY", "")


@router.post("/dbt/run")
def run_dbt(current_user: dict = Depends(get_current_user)):
    return {
        "user_id": current_user["id"],
        "dbt_status": run_dbt_build()
    }


@router.post("/internal/dbt/run")
def internal_run_dbt(x_internal_api_key: str = Header(None)):
    if not INTERNAL_API_KEY:
        raise HTTPException(status_code=500, detail="INTERNAL_API_KEY not set")

    if x_internal_api_key != INTERNAL_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid internal API key")

    return {
        "dbt_status": run_dbt_build()
    }


class StructuredReadyRequest(BaseModel):
    user_id: int
    document_id: int
    table_name: str
    status: str = "ready"

@router.post("/internal/structured/mark-ready")
def mark_structured_ready(
    req: StructuredReadyRequest,
    x_internal_api_key: str = Header(None)
):
    if not INTERNAL_API_KEY:
        raise HTTPException(status_code=500, detail="INTERNAL_API_KEY not set")

    if x_internal_api_key != INTERNAL_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid internal API key")

    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            # 1. Mark app document ready
            cur.execute("""
                UPDATE app_documents
                SET processing_status = %s,
                    processing_error = NULL,
                    updated_at = NOW()
                WHERE user_id = %s
                  AND id = %s
            """, (
                req.status,
                req.user_id,
                req.document_id,
            ))

            # 2. Mark structured dataset ready
            cur.execute("""
                UPDATE structured_datasets
                SET status = %s,
                    table_name = %s,
                    updated_at = NOW()
                WHERE user_id = %s
                  AND document_id = %s
            """, (
                req.status,
                req.table_name,
                req.user_id,
                req.document_id,
            ))

            # 3. Mark upload event ready
            cur.execute("""
                UPDATE file_upload_events
                SET status = %s
                WHERE user_id = %s
                  AND document_id = %s
            """, (
                req.status,
                req.user_id,
                req.document_id,
            ))

        conn.commit()

    return {
        "status": "updated",
        "user_id": req.user_id,
        "document_id": req.document_id,
        "table_name": req.table_name,
        "new_status": req.status,
        "updated_tables": [
            "app_documents",
            "structured_datasets",
            "file_upload_events"
        ],
    }