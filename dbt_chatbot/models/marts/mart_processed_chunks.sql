-- models/marts/mart_processed_chunks.sql
-- Final, deduplicated, enriched chunk table consumed by the embedding pipeline
-- User-aware version: keeps user_id, document_id, and file_hash for authentication isolation

WITH ranked AS (
    SELECT
        chunk_id,
        user_id,
        document_id,
        file_name,
        file_path,
        file_hash,
        chunk_index,
        chunk_text,
        char_count,
        word_count,
        file_size,
        ingested_at,

        ROW_NUMBER() OVER (
            PARTITION BY user_id, file_hash, chunk_index
            ORDER BY ingested_at DESC
        ) AS rn

    FROM {{ ref('stg_raw_chunks') }}
)

SELECT
    chunk_id,
    user_id,
    document_id,
    file_name,
    file_path,
    file_hash,
    chunk_index,
    chunk_text,
    char_count,
    COALESCE(word_count, 0) AS word_count,
    file_size,

    CASE
        WHEN COALESCE(word_count, 0) >= 100 THEN 'rich'
        WHEN COALESCE(word_count, 0) >= 30  THEN 'normal'
        ELSE 'short'
    END AS quality_tier,

    ingested_at,
    NOW() AS processed_at

FROM ranked
WHERE rn = 1
ORDER BY user_id, file_name, chunk_index