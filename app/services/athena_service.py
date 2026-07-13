import os
import time
from typing import Any

import boto3
from botocore.exceptions import ClientError


AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
ATHENA_DATABASE = os.getenv("ATHENA_DATABASE", "chatbot_lakehouse")
ATHENA_WORKGROUP = os.getenv("ATHENA_WORKGROUP", "primary")
ATHENA_OUTPUT_LOCATION = os.getenv("ATHENA_OUTPUT_LOCATION")
ATHENA_QUERY_TIMEOUT_SECONDS = int(
    os.getenv("ATHENA_QUERY_TIMEOUT_SECONDS", "90")
)

athena_client = boto3.client("athena", region_name=AWS_REGION)
glue_client = boto3.client("glue", region_name=AWS_REGION)


class AthenaQueryError(RuntimeError):
    """Raised when Athena cannot execute or complete a query."""


def quote_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def qualified_table_name(database: str, table_name: str) -> str:
    return f"{quote_identifier(database)}.{quote_identifier(table_name)}"


def start_query(
    sql: str,
    *,
    database: str | None = None,
) -> str:
    if not ATHENA_OUTPUT_LOCATION:
        raise RuntimeError(
            "ATHENA_OUTPUT_LOCATION environment variable is not set"
        )

    try:
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
    except ClientError as error:
        raise AthenaQueryError(str(error)) from error

    return response["QueryExecutionId"]


def wait_for_query(
    query_execution_id: str,
    *,
    timeout_seconds: int = ATHENA_QUERY_TIMEOUT_SECONDS,
    poll_interval_seconds: float = 1.0,
) -> dict[str, Any]:
    deadline = time.time() + timeout_seconds

    while time.time() < deadline:
        try:
            response = athena_client.get_query_execution(
                QueryExecutionId=query_execution_id
            )
        except ClientError as error:
            raise AthenaQueryError(str(error)) from error

        execution = response["QueryExecution"]
        status = execution["Status"]
        state = status["State"]

        if state == "SUCCEEDED":
            return execution

        if state in {"FAILED", "CANCELLED"}:
            reason = status.get(
                "StateChangeReason",
                f"Athena query ended with state {state}",
            )
            raise AthenaQueryError(reason)

        time.sleep(poll_interval_seconds)

    try:
        athena_client.stop_query_execution(
            QueryExecutionId=query_execution_id
        )
    except ClientError:
        pass

    raise AthenaQueryError(
        f"Athena query timed out after {timeout_seconds} seconds"
    )


def get_query_results(
    query_execution_id: str,
    *,
    max_results: int = 1000,
) -> list[dict[str, Any]]:
    max_results = max(1, min(max_results, 1000))

    try:
        response = athena_client.get_query_results(
            QueryExecutionId=query_execution_id,
            MaxResults=max_results,
        )
    except ClientError as error:
        raise AthenaQueryError(str(error)) from error

    result_set = response.get("ResultSet", {})
    rows = result_set.get("Rows", [])

    if not rows:
        return []

    column_info = result_set.get("ResultSetMetadata", {}).get(
        "ColumnInfo", []
    )
    column_names = [column["Name"] for column in column_info]

    result_rows: list[dict[str, Any]] = []

    # Athena normally returns a header row first.
    data_rows = rows[1:] if rows else []

    for row in data_rows:
        values = row.get("Data", [])
        record: dict[str, Any] = {}

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
    timeout_seconds: int = ATHENA_QUERY_TIMEOUT_SECONDS,
    max_results: int = 1000,
) -> dict[str, Any]:
    query_execution_id = start_query(sql, database=database)

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
        "data_scanned_bytes": statistics.get("DataScannedInBytes", 0),
        "execution_time_ms": statistics.get(
            "EngineExecutionTimeInMillis", 0
        ),
    }


def get_table_schema(
    database: str,
    table_name: str,
) -> list[dict[str, Any]]:
    try:
        response = glue_client.get_table(
            DatabaseName=database,
            Name=table_name,
        )
    except ClientError as error:
        raise AthenaQueryError(str(error)) from error

    table = response["Table"]
    storage_descriptor = table.get("StorageDescriptor", {})
    columns = storage_descriptor.get("Columns", [])
    partition_columns = table.get("PartitionKeys", [])

    system_columns = {
        "user_id",
        "document_id",
        "source_file_name",
        "processed_at",
    }

    return [
        {
            "column_name": item["Name"],
            "data_type": item["Type"],
            "is_system": item["Name"] in system_columns,
        }
        for item in columns + partition_columns
    ]


def get_table_sample(
    database: str,
    table_name: str,
    *,
    limit: int = 5,
) -> list[dict[str, Any]]:
    safe_limit = max(1, min(limit, 20))
    table_ref = qualified_table_name(database, table_name)
    sql = f"SELECT * FROM {table_ref} LIMIT {safe_limit}"

    result = execute_query(
        sql,
        database=database,
        max_results=safe_limit + 1,
    )
    return result["rows"]
