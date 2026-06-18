import os
from dotenv import load_dotenv

load_dotenv()

# ── Groq ──────────────────────────────────────────────────────────────────────
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")

# ── Auth / JWT ────────────────────────────────────────────────────────────────
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "change-this-secret-key")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
JWT_EXPIRE_MINUTES = int(os.getenv("JWT_EXPIRE_MINUTES", "1440"))

# ── Embedding model ───────────────────────────────────────────────────────────
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "all-MiniLM-L6-v2")

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
VECTOR_STORE_DIR = os.path.join(BASE_DIR, "vector_store")
DBT_PROJECT_DIR = os.path.join(BASE_DIR, "dbt_chatbot")

# ── PostgreSQL ────────────────────────────────────────────────────────────────
PG_HOST = os.getenv("PG_HOST")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_SCHEMA = os.getenv("PG_SCHEMA", "public")

if not PG_HOST:
    raise ValueError("PG_HOST is not set")

if not PG_DB:
    raise ValueError("PG_DB is not set")

if not PG_USER:
    raise ValueError("PG_USER is not set")

if not PG_PASSWORD:
    raise ValueError("PG_PASSWORD is not set")

PG_DSN = (
    f"host={PG_HOST} "
    f"port={PG_PORT} "
    f"dbname={PG_DB} "
    f"user={PG_USER} "
    f"password={PG_PASSWORD}"
)

# ── ChromaDB ──────────────────────────────────────────────────────────────────
CHROMA_COLLECTION = os.getenv("CHROMA_COLLECTION", "rag_documents")

# ── Chunking ──────────────────────────────────────────────────────────────────
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "500"))
CHUNK_OVERLAP = int(os.getenv("CHUNK_OVERLAP", "50"))

# ── RAG retrieval ─────────────────────────────────────────────────────────────
TOP_K = int(os.getenv("TOP_K", "5"))

# ── AWS ───────────────────────────────────────────────────────────────────────
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
S3_BUCKET = os.getenv("S3_BUCKET", "")