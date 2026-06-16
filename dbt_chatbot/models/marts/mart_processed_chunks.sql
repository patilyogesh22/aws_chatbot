-- models/marts/mart_processed_chunks.sql
-- Final, deduplicated, enriched chunk table consumed by the embedding pipeline

{{ config(materialized='table') }}

WITH ranked AS (
    SELECT
        chunk_id,
        file_name,
        file_path,
        chunk_index,
        chunk_text,
        char_count,
        word_count,
        file_size,
        ingested_at,

        -- Keep latest ingestion for duplicate (file_name, chunk_index) pairs
        ROW_NUMBER() OVER (
            PARTITION BY file_name, chunk_index
            ORDER BY ingested_at DESC
        ) AS rn

    FROM {{ ref('stg_raw_chunks') }}
)

SELECT
    chunk_id,
    file_name,
    file_path,
    chunk_index,
    chunk_text,
    char_count,
    COALESCE(word_count, 0)   AS word_count,
    file_size,

    -- Quality tier for observability / filtering
    CASE
        WHEN COALESCE(word_count, 0) >= 100 THEN 'rich'
        WHEN COALESCE(word_count, 0) >= 30  THEN 'normal'
        ELSE  'short'
    END  AS quality_tier,

    ingested_at,
    NOW()  AS processed_at

FROM ranked
WHERE rn = 1
ORDER BY file_name, chunk_index