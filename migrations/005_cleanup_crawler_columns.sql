ALTER TABLE structured_datasets
DROP COLUMN IF EXISTS processed_s3_path,
DROP COLUMN IF EXISTS glue_database_name;

ALTER TABLE file_upload_events
DROP COLUMN IF EXISTS processed_s3_path,
DROP COLUMN IF EXISTS glue_database_name;