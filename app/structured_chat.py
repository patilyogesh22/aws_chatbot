import json
import re
from decimal import Decimal
from typing import Dict, List, Tuple

import psycopg2
from groq import Groq

from app.config import PG_DSN, GROQ_API_KEY, GROQ_MODEL


client = Groq(api_key=GROQ_API_KEY)

SYSTEM_COLUMNS = {
    "user_id",
    "document_id",
    "source_file_name",
    "processed_at",
}

MAX_ROWS_RETURNED = 100


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def clean_llm_sql(sql: str) -> str:
    sql = sql.strip()
    sql = sql.replace("```sql", "").replace("```", "").strip()
    sql = sql.rstrip(";").strip()
    return sql


def is_numeric_sample(value) -> bool:
    if value is None:
        return False
    try:
        Decimal(str(value).replace(",", "").strip())
        return True
    except Exception:
        return False


def get_structured_table(user_id: int, file_name: str) -> Dict:
    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name, dataset_name, document_id
                FROM structured_datasets
                WHERE user_id = %s
                  AND file_name = %s
                ORDER BY id DESC
                LIMIT 1
            """, (user_id, file_name))

            row = cur.fetchone()

    if not row or not row[0]:
        raise ValueError("Structured table not found. Glue job may not be completed yet.")

    return {
        "table_name": row[0],
        "dataset_name": row[1],
        "document_id": row[2],
    }


def get_table_schema(table_name: str) -> List[Dict]:
    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = %s
                ORDER BY ordinal_position
            """, (table_name,))

            rows = cur.fetchall()

    if not rows:
        raise ValueError(f"Table schema not found for {table_name}")

    return [
        {
            "column_name": r[0],
            "data_type": r[1],
            "is_system": r[0] in SYSTEM_COLUMNS,
        }
        for r in rows
    ]


def get_visible_schema(schema: List[Dict]) -> List[Dict]:
    return [c for c in schema if not c.get("is_system")]


def get_column_samples(table_name: str, schema: List[Dict], user_id: int) -> Dict[str, List]:
    samples = {}

    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            for col in get_visible_schema(schema):
                col_name = col["column_name"]

                try:
                    cur.execute(f"""
                        SELECT DISTINCT {quote_ident(col_name)}
                        FROM {quote_ident(table_name)}
                        WHERE user_id = %s
                          AND {quote_ident(col_name)} IS NOT NULL
                        LIMIT 5
                    """, (user_id,))

                    values = [r[0] for r in cur.fetchall()]
                    samples[col_name] = values

                except Exception:
                    conn.rollback()
                    samples[col_name] = []

    return samples


def build_schema_context(table_name: str, schema: List[Dict], samples: Dict[str, List]) -> str:
    lines = []

    for col in get_visible_schema(schema):
        col_name = col["column_name"]
        data_type = col["data_type"]
        sample_values = samples.get(col_name, [])

        numeric_hint = ""
        if sample_values and any(is_numeric_sample(v) for v in sample_values):
            numeric_hint = " | numeric-looking"

        sample_text = ", ".join([str(v) for v in sample_values[:5]]) if sample_values else "no sample"

        lines.append(
            f"- {col_name} ({data_type}{numeric_hint}) | samples: {sample_text}"
        )

    return "\n".join(lines)


def is_column_list_question(question: str) -> bool:
    q = question.lower()
    patterns = [
        "column name",
        "columns name",
        "all columns",
        "show columns",
        "list columns",
        "dataset columns",
        "table columns",
        "field names",
    ]
    return any(p in q for p in patterns)


def answer_column_list(schema: List[Dict]) -> Dict:
    columns = [c["column_name"] for c in get_visible_schema(schema)]

    answer = "The dataset columns are:\n" + "\n".join(
        [f"{i + 1}. {c}" for i, c in enumerate(columns)]
    )

    return {
        "answer": answer,
        "sql": None,
        "table_name": None,
        "rows": [],
        "row_count": len(columns),
        "file_type": "structured",
        "sources": [],
        "chunks": [],
        "chunks_used": 0,
    }


def generate_sql(
    question: str,
    table_name: str,
    schema_context: str,
    user_id: int
) -> str:
    system_prompt = """
You are an expert PostgreSQL SQL generator for a secure structured-data chatbot.

Your job:
Generate ONE accurate PostgreSQL SELECT query that answers the user question.

Hard rules:
1. Return ONLY SQL. No markdown. No explanation.
2. Use ONLY the given table.
3. Use ONLY the provided user-visible columns plus user_id for filtering.
4. Always include: WHERE user_id = <current_user_id>
5. Never expose system columns in SELECT: user_id, document_id, source_file_name, processed_at.
6. Never use INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, TRUNCATE, GRANT, REVOKE, COPY, EXECUTE, CALL, MERGE.
7. Never query information_schema, pg_catalog, app_users, app_documents, chat_history, structured_datasets, file_upload_events, schema_migrations.
8. Do not use JOIN, UNION, INTERSECT, or EXCEPT.
9. For detail/list queries, add LIMIT 100.
10. For one-record questions, use LIMIT 1.
11. For count questions, use COUNT(*).
12. For summary questions, use COUNT, MIN, MAX, AVG, SUM, GROUP BY where useful.
13. For highest/top/maximum/largest questions:
    - Add target_column IS NOT NULL.
    - If the target column is text but numeric-looking, use CAST(REPLACE(target_column, ',', '') AS NUMERIC).
    - Sort DESC.
    - Use NULLS LAST.
14. For lowest/minimum/smallest questions:
    - Add target_column IS NOT NULL.
    - If numeric-looking text, use CAST(REPLACE(target_column, ',', '') AS NUMERIC).
    - Sort ASC.
    - Use NULLS LAST.
15. For average/sum/min/max on numeric-looking text columns, always CAST(REPLACE(column, ',', '') AS NUMERIC).
16. For date-looking text columns, use TO_DATE(column, 'YYYY-MM-DD') only if needed.
17. For text matching, use ILIKE for flexible matching.
18. If user asks "HR department information", filter department ILIKE '%HR%'.
19. If user asks by designation, filter designation ILIKE '%value%'.
20. If user asks "all details", SELECT only user-visible columns, not system columns.
21. If a column can contain NULL values, avoid returning NULL as the best/highest/lowest answer unless the user specifically asks for NULLs.
22. Prefer accurate SQL over short SQL.
"""

    user_prompt = f"""
Table:
{table_name}

Current user_id:
{user_id}

User-visible schema with samples:
{schema_context}

User question:
{question}

Generate SQL:
"""

    response = client.chat.completions.create(
        model=GROQ_MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0,
        max_tokens=900,
    )

    return clean_llm_sql(response.choices[0].message.content)


def repair_sql(
    question: str,
    table_name: str,
    schema_context: str,
    user_id: int,
    failed_sql: str,
    error_message: str
) -> str:
    system_prompt = """
You are an expert PostgreSQL SQL repair assistant.

Fix the SQL query using the error message.

Rules:
1. Return ONLY the corrected SQL.
2. Use only the provided table and columns.
3. Always filter by user_id.
4. Never expose system columns in SELECT.
5. Only SELECT queries are allowed.
6. If error is due to numeric text, use CAST(REPLACE(column, ',', '') AS NUMERIC).
7. If error is due to duplicate LIMIT, use only one LIMIT.
8. If highest/top query returned NULL risk, add column IS NOT NULL and NULLS LAST.
"""

    user_prompt = f"""
Table:
{table_name}

Current user_id:
{user_id}

Schema:
{schema_context}

Question:
{question}

Failed SQL:
{failed_sql}

PostgreSQL error:
{error_message}

Corrected SQL:
"""

    response = client.chat.completions.create(
        model=GROQ_MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0,
        max_tokens=900,
    )

    return clean_llm_sql(response.choices[0].message.content)


def validate_sql(sql: str, table_name: str) -> str:
    sql_clean = clean_llm_sql(sql)
    lowered = sql_clean.lower()

    if ";" in sql_clean:
        raise ValueError("Multiple SQL statements are not allowed.")

    if not lowered.startswith("select"):
        raise ValueError("Only SELECT queries are allowed.")

    blocked_words = [
        "insert", "update", "delete", "drop", "alter", "create",
        "truncate", "grant", "revoke", "copy", "execute", "call", "merge"
    ]

    for word in blocked_words:
        if re.search(rf"\b{word}\b", lowered):
            raise ValueError(f"Unsafe SQL keyword detected: {word}")

    blocked_sources = [
        "information_schema", "pg_catalog", "app_users", "app_documents",
        "chat_history", "structured_datasets", "file_upload_events",
        "schema_migrations"
    ]

    for source in blocked_sources:
        if source in lowered:
            raise ValueError(f"Access to restricted source is not allowed: {source}")

    if table_name.lower() not in lowered:
        raise ValueError("SQL does not use the expected table.")

    if not re.search(r"\buser_id\b", lowered):
        raise ValueError("SQL must filter by user_id.")

    blocked_patterns = [
        r"\bjoin\b",
        r"\bunion\b",
        r"\bexcept\b",
        r"\bintersect\b",
    ]

    for pattern in blocked_patterns:
        if re.search(pattern, lowered):
            raise ValueError("JOIN/UNION/EXCEPT/INTERSECT are not allowed.")

    return sql_clean


def enforce_limit(sql: str) -> str:
    sql_clean = clean_llm_sql(sql)
    lowered = sql_clean.lower()

    if re.search(r"\blimit\s+\d+\b", lowered):
        return sql_clean

    aggregate_keywords = [
        "count(", "sum(", "avg(", "min(", "max(", "group by"
    ]

    is_aggregate = any(k in lowered for k in aggregate_keywords)

    if is_aggregate:
        return sql_clean

    return f"{sql_clean} LIMIT {MAX_ROWS_RETURNED}"


def run_sql(sql: str) -> Dict:
    final_sql = enforce_limit(sql)

    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(final_sql)
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()

    result_rows = [
        dict(zip(columns, row))
        for row in rows
    ]

    return {
        "sql": final_sql,
        "columns": columns,
        "rows": result_rows,
        "row_count": len(result_rows),
    }


def summarize_sql_result(question: str, sql: str, result: Dict) -> str:
    result_json = json.dumps(result["rows"][:20], default=str, indent=2)

    system_prompt = """
You are a precise data analyst.

Rules:
1. Answer only from the SQL result.
2. Do not guess.
3. If result is empty, say no matching records were found.
4. If result value is NULL, clearly say the value is not available.
5. If the user asked highest/lowest/top and the returned value is NULL, say no valid non-null value was found.
6. Be concise and clear.
7. Do not mention SQL unless needed.
8. Use exact names and numbers from the result.
"""

    user_prompt = f"""
User question:
{question}

SQL used:
{sql}

SQL result:
{result_json}

Final answer:
"""

    response = client.chat.completions.create(
        model=GROQ_MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0,
        max_tokens=700,
    )

    return response.choices[0].message.content.strip()


def answer_structured_question(
    user_id: int,
    file_name: str,
    question: str
) -> Dict:
    table_info = get_structured_table(user_id, file_name)
    table_name = table_info["table_name"]

    schema = get_table_schema(table_name)

    if is_column_list_question(question):
        response = answer_column_list(schema)
        response["table_name"] = table_name
        response["sources"] = [file_name]
        return response

    samples = get_column_samples(table_name, schema, user_id)
    schema_context = build_schema_context(table_name, schema, samples)

    sql = generate_sql(
        question=question,
        table_name=table_name,
        schema_context=schema_context,
        user_id=user_id,
    )

    try:
        safe_sql = validate_sql(sql, table_name)
        result = run_sql(safe_sql)

    except Exception as first_error:
        repaired_sql = repair_sql(
            question=question,
            table_name=table_name,
            schema_context=schema_context,
            user_id=user_id,
            failed_sql=sql,
            error_message=str(first_error),
        )

        safe_sql = validate_sql(repaired_sql, table_name)
        result = run_sql(safe_sql)

    final_sql = result["sql"]

    answer = summarize_sql_result(
        question=question,
        sql=final_sql,
        result=result,
    )

    return {
        "answer": answer,
        "sql": final_sql,
        "table_name": table_name,
        "rows": result["rows"][:20],
        "row_count": result["row_count"],
        "file_type": "structured",
        "sources": [file_name],
        "chunks": [],
        "chunks_used": 0,
    }