import psycopg2

from typing import Optional
from fastapi import APIRouter, Depends

from app.config import PG_DSN
from app.db import get_db_connection
from app.auth import get_current_user

router = APIRouter()


@router.get("/history")
def get_history(
    file_name: Optional[str] = None,
    current_user: dict = Depends(get_current_user)
):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            if file_name:
                cur.execute("""
                    SELECT
                        question,
                        answer,
                        file_name,
                        created_at,
                        generated_sql,
                        table_name,
                        file_type,
                        result_rows,
                        result_columns,
                        row_count
                    FROM chat_history
                    WHERE user_id = %s
                      AND file_name = %s
                    ORDER BY created_at ASC
                    LIMIT 50
                """, (
                    current_user["id"],
                    file_name,
                ))
            else:
                cur.execute("""
                    SELECT
                        question,
                        answer,
                        file_name,
                        created_at,
                        generated_sql,
                        table_name,
                        file_type,
                        result_rows,
                        result_columns,
                        row_count
                    FROM chat_history
                    WHERE user_id = %s
                    ORDER BY created_at DESC
                    LIMIT 50
                """, (
                    current_user["id"],
                ))

            rows = cur.fetchall()

    return {
        "history": [
            {
                "question": r[0],
                "answer": r[1],
                "file_name": r[2],
                "created_at": r[3].isoformat() if r[3] else None,
                "generated_sql": r[4],
                "sql": r[4],
                "table_name": r[5],
                "file_type": r[6],
                "rows": r[7] or [],
                "columns": r[8] or [],
                "row_count": r[9],
            }
            for r in rows
        ]
    }