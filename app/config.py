import os
from dotenv import load_dotenv

load_dotenv()

# ── Groq ──────────────────────────────────────────────────────────────────────
GROQ_API_KEY        = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")

# ── Embedding model (local, no API cost) ─────────────────────────────────────
EMBEDDING_MODEL     = os.getenv("EMBEDDING_MODEL", "all-MiniLM-L6-v2")

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR            = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_RAW_DIR        = os.path.join(BASE_DIR, "data", "raw")
VECTOR_STORE_DIR    = os.path.join(BASE_DIR, "vector_store")
DBT_PROJECT_DIR     = os.path.join(BASE_DIR, "dbt_chatbot")

# ── PostgreSQL ────────────────────────────────────────────────────────────────
PG_HOST             = os.getenv("PG_HOST",     "localhost")
PG_PORT             = int(os.getenv("PG_PORT", "5432"))
PG_DB               = os.getenv("PG_DB",       "chatbot")
PG_USER             = os.getenv("PG_USER",     "postgres")
PG_PASSWORD         = os.getenv("PG_PASSWORD", "root")
PG_SCHEMA           = os.getenv("PG_SCHEMA",   "public")

# DSN string for psycopg2
PG_DSN = (
    f"host={PG_HOST} port={PG_PORT} dbname={PG_DB} "
    f"user={PG_USER} password={PG_PASSWORD}"
)

# ── ChromaDB ──────────────────────────────────────────────────────────────────
CHROMA_COLLECTION   = "rag_documents"

# ── Chunking ──────────────────────────────────────────────────────────────────
CHUNK_SIZE          = int(os.getenv("CHUNK_SIZE", "500"))
CHUNK_OVERLAP       = int(os.getenv("CHUNK_OVERLAP", "50"))

# ── RAG retrieval ─────────────────────────────────────────────────────────────
TOP_K               = int(os.getenv("TOP_K", "5"))

# ── AWS (used only in AWS deployment) ────────────────────────────────────────
AWS_REGION          = os.getenv("AWS_REGION", "eu-north-1")
S3_BUCKET           = os.getenv("S3_BUCKET", "")
DYNAMODB_TABLE      = os.getenv("DYNAMODB_TABLE", "rag_chunks")