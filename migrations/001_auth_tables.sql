-- 1. USERS TABLE
CREATE TABLE IF NOT EXISTS app_users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 2. DOCUMENTS TABLE
CREATE TABLE IF NOT EXISTS app_documents (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    file_name TEXT NOT NULL,
    file_hash TEXT NOT NULL,
    s3_key TEXT,
    file_size BIGINT DEFAULT 0,
    uploaded_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(user_id, file_hash)
);

-- 3. CHAT HISTORY TABLE
CREATE TABLE IF NOT EXISTS chat_history (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    question TEXT NOT NULL,
    answer TEXT NOT NULL,
    file_name TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 4. RAW CHUNKS USER COLUMNS
ALTER TABLE raw_chunks ADD COLUMN IF NOT EXISTS user_id INTEGER;
ALTER TABLE raw_chunks ADD COLUMN IF NOT EXISTS document_id INTEGER;
ALTER TABLE raw_chunks ADD COLUMN IF NOT EXISTS file_hash TEXT;

-- 5. FILE UPLOAD EVENTS USER COLUMN
ALTER TABLE file_upload_events ADD COLUMN IF NOT EXISTS user_id INTEGER;

-- 6. INDEXES FOR USER-WISE FILTERING
CREATE INDEX IF NOT EXISTS idx_raw_chunks_user_id
ON raw_chunks(user_id);

CREATE INDEX IF NOT EXISTS idx_raw_chunks_document_id
ON raw_chunks(document_id);

CREATE INDEX IF NOT EXISTS idx_raw_chunks_user_file
ON raw_chunks(user_id, file_name);

CREATE INDEX IF NOT EXISTS idx_raw_chunks_user_hash
ON raw_chunks(user_id, file_hash);

CREATE INDEX IF NOT EXISTS idx_app_documents_user_id
ON app_documents(user_id);

CREATE INDEX IF NOT EXISTS idx_app_documents_user_hash
ON app_documents(user_id, file_hash);

CREATE INDEX IF NOT EXISTS idx_chat_history_user_id
ON chat_history(user_id);

CREATE INDEX IF NOT EXISTS idx_file_upload_events_user_id
ON file_upload_events(user_id);