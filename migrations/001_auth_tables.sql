CREATE TABLE IF NOT EXISTS app_users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS app_documents (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    file_name TEXT NOT NULL,
    file_hash TEXT NOT NULL,
    file_type TEXT,
    s3_key TEXT,
    file_size BIGINT DEFAULT 0,
    uploaded_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(user_id, file_hash)
);

CREATE TABLE IF NOT EXISTS chat_history (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    question TEXT NOT NULL,
    answer TEXT NOT NULL,
    file_name TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_app_documents_user_id
ON app_documents(user_id);

CREATE INDEX IF NOT EXISTS idx_app_documents_user_hash
ON app_documents(user_id, file_hash);

CREATE INDEX IF NOT EXISTS idx_app_documents_file_type
ON app_documents(file_type);

CREATE INDEX IF NOT EXISTS idx_chat_history_user_id
ON chat_history(user_id);