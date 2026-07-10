"""Athena-backed structured chat over per-upload Apache Iceberg tables."""

import hashlib
import json
import re
import time
from decimal import Decimal
from typing import Any

from app.db import get_db_connection
from app.services.ai_fallback_service import call_ai_with_fallback
from app.services.athena_service import (
    AthenaQueryError,
    execute_query,
    get_table_sample,
    get_table_schema,
    qualified_table_name,
)
from app.services.cloudwatch_metrics import send_metric
from app.services.structured_dataset_service import get_user_iceberg_dataset
from app.utils.athena_sql_validator import (
    clean_llm_sql,
    validate_athena_sql,
)


SYSTEM_COLUMNS = {
    "user_id",
    "document_id",
    "source_file_name",
    "processed_at",
}

CACHE_TTL_HOURS = 24
SCHEMA_CONTEXT_CACHE: dict[str, dict[str, Any]] = {}
SCHEMA_CONTEXT_CACHE_TTL_SECONDS = 3600


def make_question_hash(question: str) -> str:
    normalized = re.sub(r"\s+", " ", question.lower().strip())
    return hashlib.md5(normalized.encode("utf-8")).hexdigest()


def get_cached_answer(
    user_id: int,
    document_id: int,
    file_name: str,
    question: str,
):
    question_hash = make_question_hash(question)

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    generated_sql,
                    answer,
                    result_rows,
                    result_columns,
                    row_count,
                    execution_time_ms
                FROM query_cache
                WHERE user_id = %s
                  AND document_id = %s
                  AND file_name = %s
                  AND question_hash = %s
                  AND created_at > NOW() - (%s || ' hours')::interval
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (
                    user_id,
                    document_id,
                    file_name,
                    question_hash,
                    CACHE_TTL_HOURS,
                ),
            )
            row = cur.fetchone()

    if not row:
        return None

    return {
        "answer": row[1],
        "sql": row[0],
        "table_name": None,
        "rows": row[2] or [],
        "columns": row[3] or [],
        "row_count": row[4] or 0,
        "execution_time_ms": row[5],
        "file_type": "structured",
        "sources": [file_name],
        "chunks": [],
        "chunks_used": 0,
        "from_cache": True,
    }


def save_cached_answer(
    *,
    user_id: int,
    document_id: int,
    file_name: str,
    question: str,
    sql: str,
    answer: str,
    rows: list,
    columns: list,
    row_count: int,
    execution_time_ms: int | float | None,
):
    question_hash = make_question_hash(question)

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO query_cache
                (
                    user_id,
                    document_id,
                    file_name,
                    question_hash,
                    question,
                    generated_sql,
                    answer,
                    result_rows,
                    result_columns,
                    row_count,
                    execution_time_ms,
                    created_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    %s::jsonb, %s::jsonb, %s, %s, NOW()
                )
                ON CONFLICT (user_id, document_id, question_hash)
                DO UPDATE SET
                    file_name = EXCLUDED.file_name,
                    question = EXCLUDED.question,
                    generated_sql = EXCLUDED.generated_sql,
                    answer = EXCLUDED.answer,
                    result_rows = EXCLUDED.result_rows,
                    result_columns = EXCLUDED.result_columns,
                    row_count = EXCLUDED.row_count,
                    execution_time_ms = EXCLUDED.execution_time_ms,
                    created_at = NOW()
                """,
                (
                    user_id,
                    document_id,
                    file_name,
                    question_hash,
                    question,
                    sql,
                    answer,
                    json.dumps(rows or []),
                    json.dumps(columns or []),
                    row_count,
                    execution_time_ms,
                ),
            )


def get_visible_schema(schema: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        column
        for column in schema
        if not column.get("is_system")
        and column.get("column_name") not in SYSTEM_COLUMNS
    ]


def is_numeric_sample(value: Any) -> bool:
    if value in (None, ""):
        return False
    try:
        Decimal(str(value).replace(",", "").strip())
        return True
    except Exception:
        return False


def build_column_samples(
    schema: list[dict[str, Any]],
    sample_rows: list[dict[str, Any]],
) -> dict[str, list[Any]]:
    result: dict[str, list[Any]] = {}

    for column in get_visible_schema(schema):
        name = column["column_name"]
        seen: list[Any] = []

        for row in sample_rows:
            value = row.get(name)
            if value in (None, "") or value in seen:
                continue
            seen.append(value)
            if len(seen) >= 5:
                break

        result[name] = seen

    return result


def build_schema_context(
    schema: list[dict[str, Any]],
    samples: dict[str, list[Any]],
    sample_rows: list[dict[str, Any]],
) -> str:
    lines: list[str] = []

    for column in get_visible_schema(schema):
        name = column["column_name"]
        data_type = column["data_type"]
        values = samples.get(name, [])
        numeric_hint = ""

        if values and any(is_numeric_sample(value) for value in values):
            numeric_hint = " | numeric-looking"

        sample_text = (
            ", ".join(str(value) for value in values[:5])
            if values
            else "no sample"
        )

        lines.append(
            f"- {name} ({data_type}{numeric_hint}) | samples: {sample_text}"
        )

    if sample_rows:
        visible_rows = [
            {
                key: value
                for key, value in row.items()
                if key not in SYSTEM_COLUMNS
            }
            for row in sample_rows[:3]
        ]
        lines.append("\nSample rows:")
        lines.append(json.dumps(visible_rows, default=str, indent=2))

    return "\n".join(lines)


def get_schema_context_cached(dataset: dict[str, Any]):
    database = dataset["iceberg_database"]
    table = dataset["iceberg_table"]
    cache_key = f"{database}:{table}"
    now = time.time()

    cached = SCHEMA_CONTEXT_CACHE.get(cache_key)
    if cached and now - cached["created_at"] < SCHEMA_CONTEXT_CACHE_TTL_SECONDS:
        send_metric("SchemaCacheHit", 1)
        return cached["schema"], cached["schema_context"]

    send_metric("SchemaCacheMiss", 1)

    schema = get_table_schema(database, table)
    sample_rows = get_table_sample(database, table, limit=5)
    samples = build_column_samples(schema, sample_rows)
    schema_context = build_schema_context(schema, samples, sample_rows)

    SCHEMA_CONTEXT_CACHE[cache_key] = {
        "schema": schema,
        "schema_context": schema_context,
        "created_at": now,
    }

    return schema, schema_context


def is_column_list_question(question: str) -> bool:
    lowered = question.lower()
    patterns = (
        "column name",
        "columns name",
        "all columns",
        "show columns",
        "list columns",
        "dataset columns",
        "table columns",
        "field names",
    )
    return any(pattern in lowered for pattern in patterns)


def answer_column_list(
    schema: list[dict[str, Any]],
    table_name: str,
    file_name: str,
) -> dict[str, Any]:
    columns = [
        column["column_name"]
        for column in get_visible_schema(schema)
    ]

    answer = "The dataset columns are:\n" + "\n".join(
        f"{index + 1}. {column}"
        for index, column in enumerate(columns)
    )

    return {
        "answer": answer,
        "sql": None,
        "table_name": table_name,
        "rows": [],
        "columns": columns,
        "row_count": len(columns),
        "execution_time_ms": None,
        "data_scanned_bytes": 0,
        "from_cache": False,
        "file_type": "structured",
        "sources": [file_name],
        "chunks": [],
        "chunks_used": 0,
    }


ATHENA_SQL_PROMPT = """
You are an expert Amazon Athena SQL generator for one Apache Iceberg table.

Generate exactly one read-only SQL query that answers the user's question.

Hard rules:
1. Return SQL only. Do not use markdown or explanations.
2. Query only the supplied fully qualified Iceberg table.
3. Use only supplied columns.
4. Never use INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, TRUNCATE,
   MERGE, CALL, UNLOAD, GRANT, REVOKE, MSCK, OPTIMIZE, or VACUUM.
5. Do not access information_schema or any other database/table.
6. Do not use JOIN, UNION, INTERSECT, or EXCEPT.
7. Do not select system columns unless the user explicitly asks for file metadata.
8. For detail/list queries, add LIMIT 100.
9. For one-record questions, add LIMIT 1.
10. For case-insensitive text matching, use:
    lower(CAST(column AS varchar)) LIKE '%value%'.
11. For numeric-looking text, use:
    TRY_CAST(replace(CAST(column AS varchar), ',', '') AS DOUBLE).
12. For top N per group, use ROW_NUMBER() or DENSE_RANK() in a CTE.
13. Use Amazon Athena/Trino SQL syntax, not PostgreSQL-specific syntax.
14. Prefer clear aliases for aggregate output columns.
"""


ATHENA_REPAIR_PROMPT = """
You repair one failed Amazon Athena SELECT query.

Rules:
1. Return corrected SQL only.
2. Use only the supplied Iceberg table and columns.
3. Keep the query read-only.
4. Use Athena/Trino SQL syntax.
5. Never add another table, JOIN, UNION, DDL, DML, UNLOAD, or CALL.
6. Correct invalid casts using TRY_CAST when appropriate.
7. Correct PostgreSQL-specific syntax such as ILIKE or ::type.
"""


def generate_sql(
    *,
    question: str,
    database: str,
    table_name: str,
    schema_context: str,
) -> str:
    table_ref = qualified_table_name(database, table_name)

    user_prompt = f"""
Allowed Iceberg table:
{table_ref}

User-visible schema and samples:
{schema_context}

User question:
{question}

Generate Athena SQL:
"""

    response = call_ai_with_fallback(
        messages=[
            {"role": "system", "content": ATHENA_SQL_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0,
        max_tokens=600,
    )
    return clean_llm_sql(response)


def repair_sql(
    *,
    question: str,
    database: str,
    table_name: str,
    schema_context: str,
    failed_sql: str,
    error_message: str,
) -> str:
    table_ref = qualified_table_name(database, table_name)

    user_prompt = f"""
Allowed Iceberg table:
{table_ref}

Schema:
{schema_context}

Question:
{question}

Failed SQL:
{failed_sql}

Athena error:
{error_message}

Corrected SQL:
"""

    response = call_ai_with_fallback(
        messages=[
            {"role": "system", "content": ATHENA_REPAIR_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0,
        max_tokens=600,
    )
    return clean_llm_sql(response)


def run_athena_sql(
    *,
    sql: str,
    database: str,
) -> dict[str, Any]:
    result = execute_query(
        sql,
        database=database,
        max_results=101,
    )

    rows = result["rows"]
    columns = list(rows[0].keys()) if rows else []

    return {
        "sql": sql,
        "columns": columns,
        "rows": rows,
        "row_count": len(rows),
        "execution_time_ms": result.get("execution_time_ms", 0),
        "data_scanned_bytes": result.get("data_scanned_bytes", 0),
        "query_execution_id": result.get("query_execution_id"),
    }


def summarize_sql_result(
    *,
    question: str,
    sql: str,
    result: dict[str, Any],
    file_name: str,
) -> str:
    result_json = json.dumps(result["rows"][:20], default=str, indent=2)

    system_prompt = """
You are a precise data analyst.
Answer only from the supplied Athena result.
Do not guess. If the result is empty, say no matching records were found.
Use exact names and values. Be concise because the frontend shows the result table.
"""

    user_prompt = f"""
Question:
{question}

Source file:
{file_name}

Athena SQL:
{sql}

Athena result:
{result_json}

Final answer:
"""

    return call_ai_with_fallback(
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0,
        max_tokens=400,
    )


def answer_structured_question(
    user_id: int,
    file_name: str,
    question: str,
) -> dict[str, Any]:
    dataset = get_user_iceberg_dataset(
        user_id=user_id,
        file_name=file_name,
    )

    document_id = dataset["document_id"]
    database = dataset["iceberg_database"]
    table_name = dataset["iceberg_table"]

    cached = get_cached_answer(
        user_id=user_id,
        document_id=document_id,
        file_name=file_name,
        question=question,
    )

    if cached:
        send_metric("QueryCacheHit", 1)
        cached["table_name"] = table_name
        return cached

    send_metric("QueryCacheMiss", 1)
    send_metric("StructuredQueries", 1)
    send_metric("AthenaStructuredQueries", 1)

    schema, schema_context = get_schema_context_cached(dataset)

    if is_column_list_question(question):
        return answer_column_list(schema, table_name, file_name)

    generated_sql = generate_sql(
        question=question,
        database=database,
        table_name=table_name,
        schema_context=schema_context,
    )

    try:
        safe_sql = validate_athena_sql(
            sql=generated_sql,
            allowed_database=database,
            allowed_table=table_name,
        )
        result = run_athena_sql(sql=safe_sql, database=database)

    except (ValueError, AthenaQueryError) as first_error:
        repaired_sql = repair_sql(
            question=question,
            database=database,
            table_name=table_name,
            schema_context=schema_context,
            failed_sql=generated_sql,
            error_message=str(first_error),
        )

        safe_sql = validate_athena_sql(
            sql=repaired_sql,
            allowed_database=database,
            allowed_table=table_name,
        )
        result = run_athena_sql(sql=safe_sql, database=database)

    if result["row_count"] == 0:
        answer_text = f"No records found matching your query in '{file_name}'."
    else:
        answer_text = summarize_sql_result(
            question=question,
            sql=result["sql"],
            result=result,
            file_name=file_name,
        )

    save_cached_answer(
        user_id=user_id,
        document_id=document_id,
        file_name=file_name,
        question=question,
        sql=result["sql"],
        answer=answer_text,
        rows=result["rows"][:30],
        columns=result["columns"],
        row_count=result["row_count"],
        execution_time_ms=result.get("execution_time_ms"),
    )

    return {
        "answer": answer_text,
        "sql": result["sql"],
        "table_name": table_name,
        "database_name": database,
        "rows": result["rows"][:30],
        "columns": result["columns"],
        "row_count": result["row_count"],
        "execution_time_ms": result.get("execution_time_ms"),
        "data_scanned_bytes": result.get("data_scanned_bytes", 0),
        "query_execution_id": result.get("query_execution_id"),
        "from_cache": False,
        "file_type": "structured",
        "sources": [file_name],
        "chunks": [],
        "chunks_used": 0,
    }
