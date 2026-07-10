import os
import time
from typing import Any

import boto3


AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
ATHENA_DATABASE = os.getenv("ATHENA_DATABASE", "chatbot_lakehouse")
ATHENA_WORKGROUP = os.getenv("ATHENA_WORKGROUP", "primary")
ATHENA_OUTPUT_LOCATION = os.getenv("ATHENA_OUTPUT_LOCATION")

athena_client = boto3.client(
    "athena",
    region_name=AWS_REGION,
)


class AthenaQueryError(RuntimeError):
    pass


def start_query(
    sql: str,
    *,
    database: str | None = None,
) -> str:
    if not ATHENA_OUTPUT_LOCATION:
        raise RuntimeError(
            "ATHENA_OUTPUT_LOCATION environment variable is not set"
        )

    response = athena_client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={
            "Database": database or ATHENA_DATABASE,
            "Catalog": "AwsDataCatalog",
        },
        ResultConfiguration={
            "OutputLocation": ATHENA_OUTPUT_LOCATION,
        },
        WorkGroup=ATHENA_WORKGROUP,
    )

    return response["QueryExecutionId"]


def wait_for_query(
    query_execution_id: str,
    *,
    timeout_seconds: int = 60,
    poll_interval_seconds: float = 1.0,
) -> dict[str, Any]:
    deadline = time.time() + timeout_seconds

    while time.time() < deadline:
        response = athena_client.get_query_execution(
            QueryExecutionId=query_execution_id
        )

        execution = response["QueryExecution"]
        status = execution["Status"]
        state = status["State"]

        if state == "SUCCEEDED":
            return execution

        if state in {"FAILED", "CANCELLED"}:
            reason = status.get(
                "StateChangeReason",
                "Athena query failed",
            )
            raise AthenaQueryError(reason)

        time.sleep(poll_interval_seconds)

    athena_client.stop_query_execution(
        QueryExecutionId=query_execution_id
    )

    raise AthenaQueryError(
        f"Athena query timed out after {timeout_seconds} seconds"
    )


def get_query_results(
    query_execution_id: str,
    *,
    max_results: int = 1000,
) -> list[dict[str, Any]]:
    response = athena_client.get_query_results(
        QueryExecutionId=query_execution_id,
        MaxResults=max_results,
    )

    result_set = response["ResultSet"]
    rows = result_set.get("Rows", [])

    if not rows:
        return []

    column_info = result_set["ResultSetMetadata"]["ColumnInfo"]
    column_names = [
        column["Name"]
        for column in column_info
    ]

    result_rows = []

    # The first Athena row normally contains column headers.
    for row in rows[1:]:
        values = row.get("Data", [])

        record = {}

        for index, column_name in enumerate(column_names):
            value = None

            if index < len(values):
                value = values[index].get("VarCharValue")

            record[column_name] = value

        result_rows.append(record)

    return result_rows


def execute_query(
    sql: str,
    *,
    database: str | None = None,
    timeout_seconds: int = 60,
    max_results: int = 1000,
) -> dict[str, Any]:
    query_execution_id = start_query(
        sql,
        database=database,
    )

    execution = wait_for_query(
        query_execution_id,
        timeout_seconds=timeout_seconds,
    )

    rows = get_query_results(
        query_execution_id,
        max_results=max_results,
    )

    statistics = execution.get("Statistics", {})

    return {
        "query_execution_id": query_execution_id,
        "rows": rows,
        "row_count": len(rows),
        "data_scanned_bytes": statistics.get(
            "DataScannedInBytes",
            0,
        ),
        "execution_time_ms": statistics.get(
            "EngineExecutionTimeInMillis",
            0,
        ),
    }