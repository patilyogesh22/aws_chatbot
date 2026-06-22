

CREATE TABLE IF NOT EXISTS file_upload_events (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    document_id INTEGER,
    file_name TEXT,
    s3_key TEXT,
    bucket_name TEXT,
    file_size BIGINT,
    file_type TEXT,
    dataset_name TEXT,
    table_name TEXT,
    glue_job_run_id TEXT,
    status TEXT DEFAULT 'uploaded',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_file_upload_events_user_id
ON file_upload_events(user_id);

CREATE INDEX IF NOT EXISTS idx_file_upload_events_file_type
ON file_upload_events(file_type);

CREATE INDEX IF NOT EXISTS idx_file_upload_events_status
ON file_upload_events(status);

CREATE INDEX IF NOT EXISTS idx_file_upload_events_table_name
ON file_upload_events(table_name);