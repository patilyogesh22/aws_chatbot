ALTER TABLE structured_datasets
ADD COLUMN IF NOT EXISTS iceberg_database TEXT,
ADD COLUMN IF NOT EXISTS iceberg_table TEXT,
ADD COLUMN IF NOT EXISTS iceberg_s3_path TEXT,
ADD COLUMN IF NOT EXISTS athena_output_path TEXT,
ADD COLUMN IF NOT EXISTS row_count BIGINT,
ADD COLUMN IF NOT EXISTS column_count INTEGER,
ADD COLUMN IF NOT EXISTS error_message TEXT;

ALTER TABLE file_upload_events
ADD COLUMN IF NOT EXISTS iceberg_database TEXT,
ADD COLUMN IF NOT EXISTS iceberg_table TEXT,
ADD COLUMN IF NOT EXISTS iceberg_s3_path TEXT,
ADD COLUMN IF NOT EXISTS error_message TEXT,
ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();

CREATE INDEX IF NOT EXISTS idx_structured_datasets_iceberg_table
ON structured_datasets(iceberg_database, iceberg_table);

CREATE INDEX IF NOT EXISTS idx_file_upload_events_document_id
ON file_upload_events(document_id);