CREATE TABLE IF NOT EXISTS structured_datasets (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    document_id INTEGER REFERENCES app_documents(id) ON DELETE CASCADE,
    file_name TEXT NOT NULL,
    raw_s3_key TEXT,
    processed_s3_path TEXT,
    table_name TEXT,
    status TEXT DEFAULT 'uploaded',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_structured_datasets_user_id
ON structured_datasets(user_id);

CREATE INDEX IF NOT EXISTS idx_structured_datasets_document_id
ON structured_datasets(document_id);

CREATE INDEX IF NOT EXISTS idx_structured_datasets_status
ON structured_datasets(status);