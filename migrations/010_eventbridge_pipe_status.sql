ALTER TABLE file_upload_events
ADD COLUMN IF NOT EXISTS step_function_execution_arn TEXT,
ADD COLUMN IF NOT EXISTS error_message TEXT,
ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();

ALTER TABLE structured_datasets
ADD COLUMN IF NOT EXISTS step_function_execution_arn TEXT,
ADD COLUMN IF NOT EXISTS error_message TEXT;

ALTER TABLE app_documents
ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();