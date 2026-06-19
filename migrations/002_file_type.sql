ALTER TABLE app_documents
ADD COLUMN IF NOT EXISTS file_type TEXT;

ALTER TABLE file_upload_events
ADD COLUMN IF NOT EXISTS file_type TEXT;

CREATE INDEX IF NOT EXISTS idx_app_documents_file_type
ON app_documents(file_type);

CREATE INDEX IF NOT EXISTS idx_file_upload_events_file_type
ON file_upload_events(file_type);