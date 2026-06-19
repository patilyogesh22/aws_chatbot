CREATE TABLE IF NOT EXISTS structured_datasets (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    document_id INTEGER REFERENCES app_documents(id) ON DELETE CASCADE,
    file_name TEXT NOT NULL,
    dataset_name TEXT,
    table_name TEXT,
    raw_s3_key TEXT,
    processed_s3_path TEXT,
    glue_database_name TEXT,
    glue_job_run_id TEXT,
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

ALTER TABLE file_upload_events
ADD COLUMN IF NOT EXISTS document_id INTEGER,
ADD COLUMN IF NOT EXISTS dataset_name TEXT,
ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'uploaded',
ADD COLUMN IF NOT EXISTS glue_job_run_id TEXT,
ADD COLUMN IF NOT EXISTS processed_s3_path TEXT,
ADD COLUMN IF NOT EXISTS glue_database_name TEXT,
ADD COLUMN IF NOT EXISTS table_name TEXT;