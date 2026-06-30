-- Multi-file chat support and safe chat history columns

ALTER TABLE chat_history
ADD COLUMN IF NOT EXISTS generated_sql TEXT;

ALTER TABLE chat_history
ADD COLUMN IF NOT EXISTS table_name TEXT;

ALTER TABLE chat_history
ADD COLUMN IF NOT EXISTS file_type TEXT;

ALTER TABLE chat_history
ADD COLUMN IF NOT EXISTS result_rows JSONB DEFAULT '[]'::jsonb;

ALTER TABLE chat_history
ADD COLUMN IF NOT EXISTS result_columns JSONB DEFAULT '[]'::jsonb;

ALTER TABLE chat_history
ADD COLUMN IF NOT EXISTS row_count INTEGER DEFAULT 0;

ALTER TABLE chat_history
ADD COLUMN IF NOT EXISTS file_names JSONB DEFAULT '[]'::jsonb;

ALTER TABLE chat_history
ADD COLUMN IF NOT EXISTS chat_type TEXT DEFAULT 'single';

CREATE INDEX IF NOT EXISTS idx_chat_history_file_names
ON chat_history USING GIN (file_names);

CREATE INDEX IF NOT EXISTS idx_chat_history_chat_type
ON chat_history(chat_type);
