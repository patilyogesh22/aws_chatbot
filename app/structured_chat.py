import json
import re
from typing import Dict, List

import psycopg2
from groq import Groq

from app.config import PG_DSN, GROQ_API_KEY, GROQ_MODEL


client = Groq(api_key=GROQ_API_KEY)


def get_structured_table(user_id: int, file_name: str) -> Dict:
    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    table_name,
                    dataset_name,
                    document_id
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
                SELECT
                    column_name,
                    data_type
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
        }
        for r in rows
    ]


def generate_sql(
    question: str,
    table_name: str,
    schema: List[Dict],
    user_id: int
) -> str:
    schema_text = "\n".join(
        f"- {c['column_name']} ({c['data_type']})"
        for c in schema
    )

    system_prompt = """
You are a PostgreSQL SQL generator for a secure data chatbot.

Rules:
1. Generate ONLY one SQL SELECT query.
2. Return only SQL. No markdown. No explanation.
3. Use only the provided table and provided columns.
4. Always filter by user_id.
5. Use PostgreSQL syntax.
6. Never use INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, TRUNCATE, GRANT, REVOKE, COPY, EXECUTE.
7. Never query information_schema, pg_catalog, or system tables.
8. If the question asks for a summary, generate useful aggregate SQL.
9. If numeric-looking columns are stored as text, use CAST(column AS NUMERIC) for SUM, AVG, MIN, MAX, ORDER BY numeric comparisons.
10. If date-looking columns are stored as text, use TO_DATE(column, 'YYYY-MM-DD') only when needed.
11. Add LIMIT 100 for row-level/detail queries.
12. Do not use joins.
"""

    user_prompt = f"""
Table name:
{table_name}

Current user_id:
{user_id}

Table schema:
{schema_text}

Question:
{question}

Generate PostgreSQL SELECT query:
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

    sql = response.choices[0].message.content.strip()
    sql = sql.replace("```sql", "").replace("```", "").strip()

    return sql


def validate_sql(sql: str, table_name: str) -> str:
    sql_clean = sql.strip().rstrip(";")
    lowered = sql_clean.lower()

    if ";" in sql_clean:
        raise ValueError("Multiple SQL statements are not allowed.")

    if not lowered.startswith("select"):
        raise ValueError("Only SELECT queries are allowed.")

    blocked_words = [
        "insert",
        "update",
        "delete",
        "drop",
        "alter",
        "create",
        "truncate",
        "grant",
        "revoke",
        "copy",
        "execute",
        "call",
        "merge",
    ]

    for word in blocked_words:
        if re.search(rf"\b{word}\b", lowered):
            raise ValueError(f"Unsafe SQL keyword detected: {word}")

    blocked_sources = [
        "information_schema",
        "pg_catalog",
        "pg_",
        "app_users",
        "app_documents",
        "chat_history",
        "structured_datasets",
        "file_upload_events",
        "schema_migrations",
    ]

    for source in blocked_sources:
        if source in lowered and source != table_name.lower():
            raise ValueError(f"Access to restricted table/source is not allowed: {source}")

    if table_name.lower() not in lowered:
        raise ValueError("SQL does not use the expected table.")

    if " user_id" not in lowered and "user_id" not in lowered:
        raise ValueError("SQL must filter by user_id.")

    join_patterns = [
        r"\bjoin\b",
        r"\bunion\b",
        r"\bexcept\b",
        r"\bintersect\b",
    ]

    for pattern in join_patterns:
        if re.search(pattern, lowered):
            raise ValueError("Joins, unions, except, and intersect are not allowed.")

    return sql_clean


def enforce_limit(sql: str) -> str:
    lowered = sql.lower()

    aggregate_keywords = [
        "count(",
        "sum(",
        "avg(",
        "min(",
        "max(",
        "group by",
    ]

    is_aggregate = any(k in lowered for k in aggregate_keywords)

    if " limit " in lowered:
        return sql

    if is_aggregate:
        return sql

    return f"{sql} LIMIT 100"


def run_sql(sql: str) -> Dict:
    sql = enforce_limit(sql)

    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)

            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()

    result_rows = [
        dict(zip(columns, row))
        for row in rows
    ]

    return {
        "sql": sql,
        "columns": columns,
        "rows": result_rows,
        "row_count": len(result_rows),
    }


def summarize_sql_result(question: str, sql: str, result: Dict) -> str:
    result_json = json.dumps(result["rows"][:20], default=str, indent=2)

    system_prompt = """
You are a helpful data analyst.
Answer the user's question using only the SQL result.
Be concise, accurate, and easy to understand.
If the result is empty, say no matching records were found.
Do not mention unsupported assumptions.
"""

    user_prompt = f"""
Question:
{question}

SQL:
{sql}

SQL Result:
{result_json}

Answer:
"""

    response = client.chat.completions.create(
        model=GROQ_MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.2,
        max_tokens=600,
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

    sql = generate_sql(
        question=question,
        table_name=table_name,
        schema=schema,
        user_id=user_id,
    )

    safe_sql = validate_sql(sql, table_name)

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