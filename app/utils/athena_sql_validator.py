import re


MAX_ROWS_RETURNED = 100

BLOCKED_KEYWORDS = {
    "insert",
    "update",
    "delete",
    "drop",
    "alter",
    "create",
    "truncate",
    "merge",
    "call",
    "unload",
    "grant",
    "revoke",
    "msck",
    "optimize",
    "vacuum",
    "repair",
    "use",
    "show",
    "describe",
    "explain",
}

BLOCKED_SOURCES = {
    "information_schema",
    "system",
    "pg_catalog",
    "app_users",
    "app_documents",
    "chat_history",
    "structured_datasets",
    "file_upload_events",
    "schema_migrations",
}


def clean_llm_sql(sql: str) -> str:
    cleaned = (sql or "").strip()
    cleaned = re.sub(r"^```(?:sql)?\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*```$", "", cleaned)
    return cleaned.strip().rstrip(";").strip()


def _normalise_identifier(value: str) -> str:
    return value.replace('"', "").replace("`", "").lower()


def enforce_limit(sql: str, max_rows: int = MAX_ROWS_RETURNED) -> str:
    cleaned = clean_llm_sql(sql)
    lowered = cleaned.lower()

    if re.search(r"\blimit\s+\d+\b", lowered):
        return cleaned

    aggregate_patterns = (
        "count(",
        "sum(",
        "avg(",
        "min(",
        "max(",
    )

    if any(pattern in lowered for pattern in aggregate_patterns) and "group by" not in lowered:
        return cleaned

    return f"{cleaned} LIMIT {max_rows}"


def validate_athena_sql(
    *,
    sql: str,
    allowed_database: str,
    allowed_table: str,
) -> str:
    cleaned = clean_llm_sql(sql)
    lowered = cleaned.lower()

    if not cleaned:
        raise ValueError("The AI generated an empty SQL query.")

    if ";" in cleaned:
        raise ValueError("Multiple SQL statements are not allowed.")

    if "--" in cleaned or "/*" in cleaned or "*/" in cleaned:
        raise ValueError("SQL comments are not allowed.")

    if not re.match(r"^(select|with)\b", lowered):
        raise ValueError("Only SELECT or WITH queries are allowed.")

    for keyword in BLOCKED_KEYWORDS:
        if re.search(rf"\b{re.escape(keyword)}\b", lowered):
            raise ValueError(f"Unsafe SQL keyword detected: {keyword}")

    for source in BLOCKED_SOURCES:
        if source in lowered:
            raise ValueError(
                f"Access to restricted source is not allowed: {source}"
            )

    expected_full = _normalise_identifier(
        f"{allowed_database}.{allowed_table}"
    )
    expected_table = _normalise_identifier(allowed_table)
    normalised_sql = _normalise_identifier(cleaned)

    if expected_full not in normalised_sql and expected_table not in normalised_sql:
        raise ValueError("SQL does not reference the allowed Iceberg table.")

    # Prevent references to a different explicit database.table pair.
    explicit_references = re.findall(
        r"\b(?:from|join)\s+([\"`a-zA-Z0-9_]+)\.([\"`a-zA-Z0-9_]+)",
        cleaned,
        flags=re.IGNORECASE,
    )

    for database, table in explicit_references:
        db_name = _normalise_identifier(database)
        table_name = _normalise_identifier(table)
        if db_name != allowed_database.lower() or table_name != allowed_table.lower():
            raise ValueError(
                "SQL references a database or table that is not allowed."
            )

    return enforce_limit(cleaned)