-- models/staging/stg_raw_chunks.sql
-- Cleans and validates raw ingested chunks from PostgreSQL

{{ config(materialized='view') }}

SELECT
    chunk_id,
    file_name,
    file_path,
    chunk_index,

    -- Trim whitespace
    TRIM(chunk_text)  AS chunk_text,

    -- Recalculate char/word counts on trimmed text
    LENGTH(TRIM(chunk_text))    AS char_count,

    -- PostgreSQL word count via regexp_count (PG 15+) or array_length fallback
    ARRAY_LENGTH(
        STRING_TO_ARRAY(TRIM(chunk_text), ' '),
        1
    )  AS word_count,

    file_size,

    ingested_at

FROM {{ source('raw', 'raw_chunks') }}
WHERE
    chunk_text IS NOT NULL
    AND LENGTH(TRIM(chunk_text)) > 50