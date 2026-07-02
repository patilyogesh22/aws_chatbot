"""
app/structured_chat.py
Secure structured NL-to-SQL over per-upload RDS tables.

Fixes:
- Same file name can be uploaded by multiple users.
- Uses structured_datasets table_name for the logged-in user only.
- Uses user_id + document_id filters in generated SQL.
- Hides Glue/system columns from user-facing schema and result tables.
- Supports saved schema_json/sample_json from Glue, with fallback to information_schema.
"""

import json
import hashlib
import time
import re
from decimal import Decimal
from typing import Dict, List, Optional
from datetime import date, datetime
import psycopg2
from app.db import get_db_connection

from app.config import PG_DSN, GROQ_API_KEY, GROQ_MODEL
from app.services.ai_fallback_service import call_ai_with_fallback



SYSTEM_COLUMNS = {
    "user_id",
    "document_id",
    "source_file_name",
    "processed_at",
}

MAX_ROWS_RETURNED = 100
CACHE_TTL_HOURS = 24
SCHEMA_CONTEXT_CACHE = {}
SCHEMA_CONTEXT_CACHE_TTL_SECONDS = 3600

def handle_groq_error(e: Exception):
    msg = str(e)

    if (
        "rate_limit_exceeded" in msg
        or "Rate limit reached" in msg
        or "429" in msg
        or "tokens per day" in msg
        or "TPD" in msg
    ):
        raise Exception(
            "AI quota limit reached. Please try again after some time."
        )

    raise e



def quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def clean_llm_sql(sql: str) -> str:
    sql = (sql or "").strip()
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

def make_json_safe(value):
    if isinstance(value, Decimal):
        return float(value)

    if isinstance(value, (datetime, date)):
        return value.isoformat()

    return value

def make_question_hash(question: str) -> str:
    normalized = re.sub(r"\s+", " ", question.lower().strip())
    return hashlib.md5(normalized.encode("utf-8")).hexdigest()


def get_cached_answer(user_id: int, document_id: int, file_name: str, question: str):
    question_hash = make_question_hash(question)

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT generated_sql, answer, result_rows, result_columns, row_count, execution_time_ms
                FROM query_cache
                WHERE user_id = %s
                  AND document_id = %s
                  AND file_name = %s
                  AND question_hash = %s
                  AND created_at > NOW() - (%s || ' hours')::interval
                ORDER BY created_at DESC
                LIMIT 1
            """, (
                user_id,
                document_id,
                file_name,
                question_hash,
                CACHE_TTL_HOURS,
            ))

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
    execution_time_ms,
):
    question_hash = make_question_hash(question)

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
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
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, NOW())
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
            """, (
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
            ))
        print("[cache] Saving answer to query_cache")
        print("user_id:", user_id)
        print("document_id:", document_id)
        print("file_name:", file_name)
        print("question:", question)
    print("[cache] Saved successfully")

def get_structured_table(user_id: int, file_name: str) -> Dict:
    """
    Return the structured table for this exact logged-in user's selected file.

    Important:
    Glue now creates unique table names such as:
        u1_d48_employee_master_prod
        u2_d55_employee_master_prod

    So two users can upload the same file name without sharing one table.
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    sd.table_name,
                    sd.dataset_name,
                    sd.document_id,
                    COALESCE(sd.status, 'glue_job_pending') AS status,
                    sd.schema_json,
                    sd.sample_json,
                    sd.row_count
                FROM structured_datasets sd
                JOIN app_documents ad
                  ON ad.id = sd.document_id
                 AND ad.user_id = sd.user_id
                WHERE sd.user_id = %s
                  AND sd.file_name = %s
                ORDER BY sd.id DESC
                LIMIT 1
            """, (user_id, file_name))

            row = cur.fetchone()

    if not row:
        raise ValueError(
            f"No structured dataset found for '{file_name}'. "
            "Please upload the file first."
        )

    table_name, dataset_name, document_id, status, schema_json, sample_json, row_count = row

    if status in ("glue_job_pending", "glue_job_started"):
        messages = {
            "glue_job_pending": (
                "File uploaded successfully. AWS Glue is starting. "
                "Please wait 1–3 minutes and try again."
            ),
            "glue_job_started": (
                "AWS Glue is processing your file. "
                "Please wait 1–2 minutes and try again."
            ),
        }
        raise ValueError(messages.get(status, f"File status is {status}. Please wait."))

    if not table_name:
        raise ValueError(
            f"Structured table is not ready for '{file_name}' "
            f"(status: {status}). Please wait or re-upload the file."
        )

    return {
        "table_name": table_name,
        "dataset_name": dataset_name,
        "document_id": document_id,
        "status": status,
        "schema_json": schema_json or {},
        "sample_json": sample_json or [],
        "row_count": row_count or 0,
    }

def get_schema_context_cached(
    *,
    table_name: str,
    table_info: Dict,
    user_id: int,
    document_id: int,
):
    cache_key = f"{user_id}:{document_id}:{table_name}"
    now = time.time()

    cached = SCHEMA_CONTEXT_CACHE.get(cache_key)

    if cached and now - cached["created_at"] < SCHEMA_CONTEXT_CACHE_TTL_SECONDS:
        print("[schema cache] HIT")
        return cached["schema"], cached["schema_context"]

    print("[schema cache] MISS")

    schema = schema_from_json(table_info.get("schema_json") or {})

    if not schema:
        schema = get_table_schema(table_name)

    sample_rows = normalize_sample_json(table_info.get("sample_json") or [])

    samples = get_column_samples_batch(
        table_name=table_name,
        schema=schema,
        user_id=user_id,
        document_id=document_id,
    )

    schema_context = build_schema_context(
        schema=schema,
        samples=samples,
        sample_rows=sample_rows,
    )

    SCHEMA_CONTEXT_CACHE[cache_key] = {
        "schema": schema,
        "schema_context": schema_context,
        "created_at": now,
    }

    return schema, schema_context

def get_table_schema(table_name: str) -> List[Dict]:
    """
    Read schema from PostgreSQL and mark internal Glue columns.
    """
    with get_db_connection() as conn:
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


def schema_from_json(schema_json: Dict) -> List[Dict]:
    """
    Convert Glue stored schema_json into normal schema list.
    schema_json format is expected as: {"column_name": "data_type"}
    """
    result = []

    for col_name, data_type in (schema_json or {}).items():
        if col_name in SYSTEM_COLUMNS:
            continue

        result.append({
            "column_name": col_name,
            "data_type": data_type,
            "is_system": False,
        })

    return result


def get_visible_schema(schema: List[Dict]) -> List[Dict]:
    return [c for c in schema if not c.get("is_system") and c["column_name"] not in SYSTEM_COLUMNS]


def get_column_samples_batch(table_name: str, schema: List[Dict], user_id: int, document_id: int) -> Dict[str, List]:
    visible = get_visible_schema(schema)
    if not visible:
        return {}
    
    # Build one SELECT with all columns
    col_exprs = ", ".join([
        f"(SELECT array_agg(DISTINCT {quote_ident(c['column_name'])}) "
        f"FROM (SELECT {quote_ident(c['column_name'])} FROM {quote_ident(table_name)} "
        f"WHERE user_id = {user_id} AND document_id = {document_id} "
        f"AND {quote_ident(c['column_name'])} IS NOT NULL LIMIT 50) s) AS {quote_ident(c['column_name'])}"
        for c in visible
    ])
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT {col_exprs}")
            row = cur.fetchone()
            col_names = [c["column_name"] for c in visible]
            return {
                col: (list(vals[:5]) if vals else [])
                for col, vals in zip(col_names, row or [])
            }

def normalize_sample_json(sample_json: List[Dict]) -> List[Dict]:
    cleaned = []
    for row in sample_json or []:
        cleaned.append({
            k: v
            for k, v in row.items()
            if k not in SYSTEM_COLUMNS
        })
    return cleaned


def build_schema_context(schema: List[Dict], samples: Dict[str, List], sample_rows: Optional[List[Dict]] = None) -> str:
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

    if sample_rows:
        lines.append("\nSample rows:")
        lines.append(json.dumps(sample_rows[:3], default=str, indent=2))

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


def answer_column_list(schema: List[Dict], table_name: str, file_name: str) -> Dict:
    columns = [c["column_name"] for c in get_visible_schema(schema)]

    answer = "The dataset columns are:\n" + "\n".join(
        [f"{i + 1}. {c}" for i, c in enumerate(columns)]
    )

    return {
        "answer": answer,
        "sql": None,
        "table_name": table_name,
        "rows": [],
        "columns": columns,
        "row_count": len(columns),
        "execution_time_ms": None,
        "from_cache": False,
        "file_type": "structured",
        "sources": [file_name],
        "chunks": [],
        "chunks_used": 0,
    }




SIMPLE_SQL_PROMPT = """
You are a PostgreSQL SQL generator for simple structured-data questions.

Rules:
1. Return ONLY SQL. No markdown. No explanation.
2. Use ONLY the given table.
3. Always include BOTH filters:
   WHERE user_id = <current_user_id>
   AND document_id = <current_document_id>
4. Never expose system columns in SELECT: user_id, document_id, source_file_name, processed_at.
5. Only SELECT queries are allowed.
6. Never use INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, TRUNCATE, GRANT, REVOKE, COPY, EXECUTE, CALL, MERGE.
7. For simple count questions, use COUNT(*).
8. For simple total/sum/average/min/max questions, use SUM, AVG, MIN, or MAX.
9. For numeric-looking text columns, use CAST(REPLACE(column, ',', '') AS NUMERIC).
10. Keep the SQL short and efficient.
"""


FULL_SQL_PROMPT = """
You are an expert PostgreSQL SQL generator for a secure structured-data chatbot.

Your job:
Generate ONE accurate PostgreSQL SELECT query that answers the user question.

Hard rules:
1. Return ONLY SQL. No markdown. No explanation.
2. Use ONLY the given table.
3. Use ONLY the provided user-visible columns plus user_id and document_id for filtering.
4. Always include BOTH filters:
   WHERE user_id = <current_user_id>
   AND document_id = <current_document_id>
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
23. For "top N per group" questions, such as "top 3 employees in each department":
    - Use a CTE or subquery.
    - Use ROW_NUMBER() or DENSE_RANK() OVER (PARTITION BY group_column ORDER BY metric_column DESC).
    - Filter rank <= N in the outer query.
    - Never use only ORDER BY + LIMIT for "top N per department/category/city/group".
24. If the user says "each", "per", "by department", "by category", "by city", or "group wise", use GROUP BY or PARTITION BY.
25. Do not use LIMIT for top N per group except final safety LIMIT if needed.
"""


def classify_question_complexity(question: str) -> str:
    q = question.lower()

    if is_column_list_question(question):
        return "column_list"

    simple_terms = ["how many", "count", "total", "sum of", "average", "avg", "minimum", "maximum"]
    group_terms = ["by", "per", "each", "group", "department", "category", "city", "state"]

    if any(term in q for term in simple_terms):
        if not any(term in q for term in group_terms):
            return "simple"

    complex_terms = [
        "top", "highest", "lowest", "per", "each", "rank", "best",
        "group", "by", "department wise", "category wise", "city wise"
    ]

    if any(term in q for term in complex_terms):
        return "complex"

    return "standard"

def generate_sql(
    question: str,
    table_name: str,
    schema_context: str,
    user_id: int,
    document_id: int,
) -> str:
    complexity = classify_question_complexity(question)
    print(f"[SQL Complexity] {complexity}")
    print(f"[SQL] max_tokens={max_tokens}")
    if complexity == "simple":
        system_prompt = SIMPLE_SQL_PROMPT
        max_tokens = 200
    else:
        system_prompt = FULL_SQL_PROMPT
        max_tokens = 500

    print(f"[sql generator] complexity={complexity}, max_tokens={max_tokens}")

    user_prompt = f"""
Table:
{table_name}

Current user_id:
{user_id}

Current document_id:
{document_id}

User-visible schema with samples:
{schema_context}

User question:
{question}

Generate SQL:
"""

    content = call_ai_with_fallback(
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0,
        max_tokens=max_tokens,
    )

    return clean_llm_sql(content)

def repair_sql(
    question: str,
    table_name: str,
    schema_context: str,
    user_id: int,
    document_id: int,
    failed_sql: str,
    error_message: str,
) -> str:
    system_prompt = """
You are an expert PostgreSQL SQL repair assistant.

Fix the SQL query using the error message.

Rules:
1. Return ONLY the corrected SQL.
2. Use only the provided table and columns.
3. Always filter by BOTH user_id and document_id.
4. Never expose system columns in SELECT.
5. Only SELECT queries are allowed.
6. If error is due to numeric text, use CAST(REPLACE(column, ',', '') AS NUMERIC).
7. If error is due to duplicate LIMIT, use only one LIMIT.
8. If highest/top query returned NULL risk, add column IS NOT NULL and NULLS LAST.
9. For "top N per group" questions, use a CTE/subquery with ROW_NUMBER() or DENSE_RANK()
   OVER (PARTITION BY group_column ORDER BY metric_column DESC), then filter rank <= N.
10. Never use only ORDER BY + LIMIT for "top N per department/category/city/group".
"""

    user_prompt = f"""
Table:
{table_name}

Current user_id:
{user_id}

Current document_id:
{document_id}

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

    content = call_ai_with_fallback(
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0,
        max_tokens=500,
    )

    return clean_llm_sql(content)


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

    if not re.search(r"\bdocument_id\b", lowered):
        raise ValueError("SQL must filter by document_id.")

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

    start_time = time.time()

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(final_sql)
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()

    execution_time_ms = round((time.time() - start_time) * 1000, 2)

    result_rows = []

    for row in rows:
        item = {}

        for column, value in zip(columns, row):
            item[column] = make_json_safe(value)

        result_rows.append(item)

    return {
        "sql": final_sql,
        "columns": columns,
        "rows": result_rows,
        "row_count": len(result_rows),
        "execution_time_ms": execution_time_ms,
    }


def summarize_sql_result(question: str, sql: str, result: Dict, file_name: str) -> str:
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
9. For list/detail questions, summarize briefly because the frontend will show the table.
"""

    user_prompt = f"""
User question:
{question}

Source file:
{file_name}

SQL used:
{sql}

SQL result:
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
) -> Dict:
    table_info = get_structured_table(user_id, file_name)
    table_name = table_info["table_name"]
    document_id = table_info["document_id"]
    cached = get_cached_answer(
        user_id=user_id,
        document_id=document_id,
        file_name=file_name,
        question=question,
    )

    if cached:
        cached["table_name"] = table_name
        return cached

    schema, schema_context = get_schema_context_cached(
        table_name=table_name,
        table_info=table_info,
        user_id=user_id,
        document_id=document_id,
    )

    if is_column_list_question(question):
        return answer_column_list(schema, table_name, file_name)

    sql = generate_sql(
        question=question,
        table_name=table_name,
        schema_context=schema_context,
        user_id=user_id,
        document_id=document_id,
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
            document_id=document_id,
            failed_sql=sql,
            error_message=str(first_error),
        )

        safe_sql = validate_sql(repaired_sql, table_name)
        result = run_sql(safe_sql)

    final_sql = result["sql"]

    if result["row_count"] == 0:
        answer_text = f"No records found matching your query in '{file_name}'."
    else:
        answer_text = summarize_sql_result(
            question=question,
            sql=final_sql,
            result=result,
            file_name=file_name,
        )

    save_cached_answer(
        user_id=user_id,
        document_id=document_id,
        file_name=file_name,
        question=question,
        sql=final_sql,
        answer=answer_text,
        rows=result["rows"][:30],
        columns=result["columns"],
        row_count=result["row_count"],
        execution_time_ms=result.get("execution_time_ms"),
    )
    return {
        "answer": answer_text,
        "sql": final_sql,
        "table_name": table_name,
        "rows": result["rows"][:30],
        "columns": result["columns"],
        "row_count": result["row_count"],
        "execution_time_ms": result.get("execution_time_ms"),
        "from_cache": False,
        "file_type": "structured",
        "sources": [file_name],
        "chunks": [],
        "chunks_used": 0,
    }
