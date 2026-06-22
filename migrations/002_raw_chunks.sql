CREATE TABLE IF NOT EXISTS raw_chunks (
    chunk_id TEXT PRIMARY KEY,
    user_id INTEGER,
    document_id INTEGER,
    file_name TEXT NOT NULL,
    file_path TEXT,
    file_hash TEXT,
    chunk_index INTEGER NOT NULL,
    chunk_text TEXT NOT NULL,
    char_count INTEGER,
    word_count INTEGER,
    file_size BIGINT DEFAULT 0,
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_chunks_file_name
ON raw_chunks(file_name);

CREATE INDEX IF NOT EXISTS idx_raw_chunks_file_hash
ON raw_chunks(file_hash);

CREATE INDEX IF NOT EXISTS idx_raw_chunks_user_id
ON raw_chunks(user_id);

CREATE INDEX IF NOT EXISTS idx_raw_chunks_document_id
ON raw_chunks(document_id);

CREATE INDEX IF NOT EXISTS idx_raw_chunks_user_file
ON raw_chunks(user_id, file_name);

CREATE INDEX IF NOT EXISTS idx_raw_chunks_user_hash
ON raw_chunks(user_id, file_hash);