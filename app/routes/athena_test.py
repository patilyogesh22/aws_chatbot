from fastapi import APIRouter, Depends, HTTPException

from app.auth import get_current_user
from app.services.athena_service import (
    AthenaQueryError,
    execute_query,
)


router = APIRouter(
    prefix="/athena",
    tags=["Athena"],
)


@router.get("/test/{table_name}")
def test_athena_table(
    table_name: str,
    current_user: dict = Depends(get_current_user),
):
    expected_prefix = f"u{current_user['id']}_"

    if not table_name.startswith(expected_prefix):
        raise HTTPException(
            status_code=403,
            detail="You cannot query another user's table",
        )

    if not table_name.replace("_", "").isalnum():
        raise HTTPException(
            status_code=400,
            detail="Invalid table name",
        )

    sql = f"""
        SELECT *
        FROM chatbot_lakehouse.{table_name}
        LIMIT 10
    """

    try:
        return execute_query(sql)

    except AthenaQueryError as error:
        raise HTTPException(
            status_code=400,
            detail=str(error),
        ) from error