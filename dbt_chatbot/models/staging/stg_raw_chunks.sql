-- models/staging/stg_raw_chunks.sql
-- Cleans and validates raw ingested chunks from PostgreSQL
-- User-aware staging model


SELECT
    chunk_id,
    user_id,
    document_id,
    file_name,
    file_path,
    file_hash,
    chunk_index,

    TRIM(chunk_text) AS chunk_text,

    LENGTH(TRIM(chunk_text)) AS char_count,

    ARRAY_LENGTH(
        STRING_TO_ARRAY(TRIM(chunk_text), ' '),
        1
    ) AS word_count,

    file_size,
    ingested_at

FROM {{ source('raw', 'raw_chunks') }}

WHERE
    chunk_text IS NOT NULL
    AND LENGTH(TRIM(chunk_text)) > 50
    AND user_id IS NOT NULL