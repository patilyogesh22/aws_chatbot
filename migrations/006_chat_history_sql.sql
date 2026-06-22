ALTER TABLE chat_history
ADD COLUMN IF NOT EXISTS generated_sql TEXT;

ALTER TABLE chat_history
ADD COLUMN IF NOT EXISTS table_name TEXT;

ALTER TABLE chat_history
ADD COLUMN IF NOT EXISTS file_type TEXT;

CREATE INDEX IF NOT EXISTS idx_chat_history_file_name
ON chat_history(file_name);

CREATE INDEX IF NOT EXISTS idx_chat_history_file_type
ON chat_history(file_type);