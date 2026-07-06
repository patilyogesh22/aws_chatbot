# ⚡ DocChat — AI-Powered Document Chat

> Upload any document and have a real conversation with it. Powered by RAG, Groq LLM, AWS data pipelines, and pgvector semantic search.

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Features](#features)
- [Project Structure](#project-structure)
- [Database Schema](#database-schema)
- [API Endpoints](#api-endpoints)
- [AWS Services Used](#aws-services-used)
- [Data Pipeline Flow](#data-pipeline-flow)
- [Setup & Installation](#setup--installation)
- [Environment Variables](#environment-variables)
- [Running Locally](#running-locally)
- [Running with Docker](#running-with-docker)
- [Deploying to AWS EC2](#deploying-to-aws-ec2)
- [dbt Setup](#dbt-setup)
- [Migrations](#migrations)
- [Frontend](#frontend)

---

## Overview

DocChat is a full-stack Retrieval-Augmented Generation (RAG) chatbot that lets users upload documents and ask questions in plain English. It handles two completely different file types through separate pipelines:

- **Unstructured files** (PDF, DOCX, TXT, MD, PPTX) → chunked → embedded → pgvector semantic search → Groq LLM answer
- **Structured files** (CSV, XLSX, JSON) → AWS Glue ETL → RDS PostgreSQL table → NL-to-SQL → Groq LLM answer

Every user's data is fully isolated — no user can access another user's files, tables, or chat history.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        FRONTEND                              │
│          index.html + style.css + app.js                     │
│    Landing Page → Auth Modals → Chat UI + File Manager       │
└─────────────────────────┬───────────────────────────────────┘
                          │ HTTPS
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                    FastAPI (EC2)                              │
│   /auth   /upload   /chat   /files   /history   /stats       │
│   JWT Auth · Connection Pool · Request Logging               │
└──────┬────────────────────────────────────┬─────────────────┘
       │                                    │
       ▼                                    ▼
┌──────────────┐                  ┌─────────────────────┐
│   AWS S3     │                  │   AWS SQS           │
│  File store  │                  │   Message Queue     │
│  per user    │                  └──────────┬──────────┘
└──────────────┘                             │
                                             ▼
                                  ┌─────────────────────┐
                                  │   AWS Lambda        │
                                  │   S3 Trigger        │
                                  │   Routes by         │
                                  │   file type         │
                                  └──────┬────────┬─────┘
                                         │        │
                          ┌──────────────┘        └──────────────┐
                          ▼                                       ▼
               ┌──────────────────┐                 ┌────────────────────┐
               │  AWS Step        │                 │  AWS Step          │
               │  Functions       │                 │  Functions         │
               │  (Unstructured)  │                 │  (Structured)      │
               └────────┬─────────┘                 └─────────┬──────────┘
                        │                                     │
                        ▼                                     ▼
               ┌──────────────────┐                 ┌────────────────────┐
               │  ECS Fargate     │                 │  AWS Glue          │
               │  Ingestion       │                 │  ETL Job           │
               │  Worker          │                 │  (PySpark)         │
               └────────┬─────────┘                 └─────────┬──────────┘
                        │                                     │
                        ▼                                     ▼
               ┌──────────────────────────────────────────────────────┐
               │              AWS RDS PostgreSQL                       │
               │                                                       │
               │  raw_chunks            ← unstructured chunks          │
               │  mart_processed_chunks ← dbt transformed chunks       │
               │  document_embeddings   ← pgvector (384 dimensions)    │
               │  u{uid}_d{did}_table   ← per-user structured tables   │
               │  chat_history          ← all conversations            │
               │  app_documents         ← file metadata                │
               │  structured_datasets   ← Glue job tracking            │
               └──────────────────────────────────────────────────────┘
                        │
                        ▼
               ┌──────────────────┐
               │  dbt (scheduled) │
               │  raw → staging   │
               │  → mart          │
               │  (every hour via │
               │  EventBridge)    │
               └──────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| **API** | FastAPI 0.111, Python 3.11, Uvicorn |
| **Auth** | JWT (python-jose), bcrypt passwords |
| **AI / LLM** | Groq (llama-3.3-70b-versatile) + Gemini fallback |
| **Embeddings** | sentence-transformers (all-MiniLM-L6-v2, 384 dims) |
| **Vector DB** | pgvector extension on RDS PostgreSQL |
| **Relational DB** | AWS RDS PostgreSQL (primary DB for everything) |
| **Object Storage** | AWS S3 (file uploads, per-user prefixes) |
| **Queue** | AWS SQS (decouples upload from processing) |
| **ETL** | AWS Glue (PySpark for structured files) |
| **Containers** | AWS ECS Fargate (unstructured file processing) |
| **Orchestration** | AWS Step Functions (coordinates Glue + ECS) |
| **Event Trigger** | AWS Lambda (S3 events → routing) |
| **Scheduling** | AWS EventBridge (hourly dbt runs) |
| **Data Transform** | dbt-core + dbt-postgres |
| **Frontend** | Vanilla HTML + CSS + JavaScript (no framework) |
| **Containerisation** | Docker + Docker Compose |

---

## Features

### Core Features
- **JWT Authentication** — Register, login, secure sessions (24hr expiry)
- **Single file upload** — `POST /upload`
- **Batch upload** — Up to 20 files at once via `POST /upload/batch`
- **Retry failed uploads** — `POST /upload/{document_id}/retry-queue`
- **Duplicate detection** — MD5 hash comparison prevents re-uploading same file
- **File delete** — Removes from S3, RDS, pgvector, structured tables, chat history in one call

### Chat Features
- **Single file chat** — Ask questions about one specific file
- **Multi-file chat** — Ask a question across multiple files simultaneously (structured + unstructured in one response)
- **Chat history** — All conversations saved to PostgreSQL, filterable by file
- **Structured query details** — Shows generated SQL, table name, row count in response
- **AI fallback chain** — Groq primary → Groq fallback model → Gemini if all fail

### Unstructured Pipeline (PDF, DOCX, TXT, MD, PPTX)
- Text extraction per file type (pdfplumber, python-docx, python-pptx)
- Chunking with overlap (500 chars, 50 overlap, configurable)
- Stored in `raw_chunks` table
- dbt transforms to `mart_processed_chunks` (deduplication, quality scoring)
- Embeddings generated via sentence-transformers, stored in pgvector
- Semantic similarity search at query time (cosine similarity, HNSW index)

### Structured Pipeline (CSV, XLSX, JSON)
- XLSX automatically converted to CSV before upload
- File stored in S3 under `uploads/user_{id}/structured/`
- Lambda triggers Step Functions → Glue ETL job
- Glue cleans columns, adds metadata, writes to unique RDS table per user+document
- NL-to-SQL: LLM generates SQL from question + schema + sample data
- SQL validated (blocks DROP, INSERT, UPDATE, JOIN across tables, etc.)
- Automatic SQL repair on failure (retry with error message fed back to LLM)
- Result summarised by LLM in plain English

### Performance Optimisations
- **Connection pooling** — `ThreadedConnectionPool` (2–10 connections) for all DB access
- **HNSW vector index** — faster similarity search than IVFFlat at all dataset sizes
- **Schema context cache** — 1hr in-memory cache per user+document (avoids repeated DB schema lookups)
- **Query embedding cache** — `lru_cache(maxsize=256)` on sentence-transformer encode
- **Batch column samples** — Single SQL query with `array_agg(DISTINCT)` instead of N queries
- **Question complexity classifier** — Routes simple questions to lightweight prompt (fewer tokens)
- **Empty result fast-path** — Skips LLM summarisation call when SQL returns 0 rows
- **Query result cache** — MD5 hash of question → cached answer (24hr TTL) in `query_cache` table

### File Status Tracking
- Every file has a `processing_status` tracked in `app_documents`
- Structured files additionally tracked in `structured_datasets`
- Frontend polls `GET /structured/status/{file_name}` every 8 seconds
- Status values: `upload_saved → sqs_queued → glue_job_pending → glue_job_started → ready / error`

---

## Project Structure

```
aws_chatbot/
│
├── app/                            # FastAPI application
│   ├── main.py                     # App entry point, middleware, router registration
│   ├── auth.py                     # JWT auth, register/login endpoints
│   ├── config.py                   # All env vars and config constants
│   ├── db.py                       # PostgreSQL connection pool
│   │
│   ├── middleware/
│   │   └── logging.py              # Request logging middleware
│   │
│   ├── routes/
│   │   ├── chat.py                 # /chat — single + multi-file chat
│   │   ├── upload.py               # /upload, /upload/batch, /upload/{id}/retry-queue
│   │   ├── files.py                # /files, /files/{name}, /structured/status/{name}
│   │   ├── history.py              # /history — chat history per user/file
│   │   ├── stats.py                # /stats — file counts, chunk counts, vectors
│   │   ├── health.py               # /health — DB connectivity check
│   │   └── internal_dbt.py         # /internal/dbt/run — triggered by EventBridge
│   │
│   ├── services/
│   │   ├── ingestion_service.py    # Text extraction, chunking, raw_chunks storage
│   │   ├── embedding_service.py    # pgvector init, embed_and_store, delete_embeddings
│   │   ├── rag_service.py          # Vector similarity search, context building
│   │   ├── llm_service.py          # RAG answer generation, multi-file synthesis
│   │   ├── structured_chat_service.py  # NL-to-SQL full pipeline
│   │   ├── ai_fallback_service.py  # Groq → Groq fallback → Gemini chain
│   │   ├── dbt_service.py          # dbt build runner
│   │   ├── queue_service.py        # SQS send message
│   │   ├── file_delete_service.py  # Delete from all 8 locations
│   │   └── startup_service.py      # DB init on API startup
│   │
│   └── utils/
│       ├── file_classifier.py      # Classify file as structured/unstructured/unknown
│       └── structured_converter.py # XLSX → CSV conversion
│
├── aws/
│   ├── ecs_ingestion_worker.py     # ECS task: ingest → embed → mark ready
│   ├── s3_ingestion.py             # S3 upload/download/delete helpers
│   │
│   ├── glue/
│   │   └── glue_job.py             # PySpark ETL: S3 CSV → clean → RDS table
│   │
│   ├── lambda_s3_trigger/
│   │   ├── lambda_handler.py       # S3 event → classify → start Glue or ECS
│   │   └── requirements-lambda.txt
│   │
│   └── lambda_run_dbt_after_glue/
│       ├── lambda_handler.py       # Glue success event → mark ready → run dbt
│       └── requirements-lambda.txt
│
├── dbt_chatbot/                    # dbt project
│   ├── dbt_project.yml
│   ├── profiles.yml                # DB connection (uses env vars)
│   ├── macros/
│   │   └── timestamps.sql
│   └── models/
│       ├── staging/
│       │   └── stg_raw_chunks.sql  # Clean + validate raw chunks
│       └── marts/
│           └── mart_processed_chunks.sql  # Deduplicate + enrich chunks
│
├── migrations/                     # SQL migrations (run in order)
│   ├── 001_auth_tables.sql
│   ├── 002_raw_chunks.sql
│   ├── 003_structured_datasets.sql
│   ├── 004_file_upload_event.sql
│   ├── 005_cleanup_crawler_columns.sql
│   ├── 006_chat_history_sql.sql
│   ├── 007_unique_structured_dataset.sql
│   ├── 008_file_processing_status.sql
│   ├── 009_multi_file_chat.sql
│   └── run_migrations.py
│
├── frontend/                       # Static frontend (served by FastAPI)
│   ├── index.html
│   ├── style.css
│   └── app.js
│
├── docker/
│   ├── Dockerfile                  # API container
│   ├── Dockerfile.ingestion        # ECS worker container (pre-baked ML model)
│   ├── docker-compose.yml
│   └── profiles.yml                # dbt profiles for Docker
│
├── data/
│   └── raw/                        # Local file staging (not committed)
│
├── requirements-core.txt           # API dependencies
├── requirements-ml.txt             # ML/embedding dependencies
└── .env                            # Environment variables (not committed)
```

---

## Database Schema

### `app_users`
| Column | Type | Description |
|---|---|---|
| id | SERIAL PK | User ID |
| name | TEXT | Display name |
| email | TEXT UNIQUE | Login email |
| password_hash | TEXT | bcrypt hash |
| created_at | TIMESTAMPTZ | Registration time |

### `app_documents`
| Column | Type | Description |
|---|---|---|
| id | SERIAL PK | Document ID |
| user_id | INTEGER FK | Owner |
| file_name | TEXT | Original filename |
| file_hash | TEXT | MD5 for duplicate detection |
| file_type | TEXT | `structured` or `unstructured` |
| s3_key | TEXT | S3 object path |
| file_size | BIGINT | Bytes |
| processing_status | TEXT | Current status |
| uploaded_at | TIMESTAMPTZ | Upload time |

### `raw_chunks`
| Column | Type | Description |
|---|---|---|
| chunk_id | TEXT PK | Unique chunk identifier |
| user_id | INTEGER | Owner |
| document_id | INTEGER | Source document |
| file_name | TEXT | Source file |
| chunk_text | TEXT | Chunk content |
| chunk_index | INTEGER | Position in document |
| word_count | INTEGER | Words in chunk |
| file_hash | TEXT | Source file hash |
| ingested_at | TIMESTAMPTZ | When created |

### `document_embeddings`
| Column | Type | Description |
|---|---|---|
| id | SERIAL PK | - |
| user_id | INTEGER | Owner |
| document_id | INTEGER | Source document |
| file_name | TEXT | Source file |
| chunk_id | TEXT | Corresponding chunk |
| chunk_text | TEXT | Original text |
| embedding | vector(384) | pgvector embedding |
| created_at | TIMESTAMPTZ | When created |

### `structured_datasets`
| Column | Type | Description |
|---|---|---|
| id | SERIAL PK | - |
| user_id | INTEGER | Owner |
| document_id | INTEGER | Source document |
| file_name | TEXT | Original filename |
| table_name | TEXT | RDS table name (`u{uid}_d{did}_name`) |
| status | TEXT | Glue job status |
| schema_json | JSONB | Column names + types |
| sample_json | JSONB | First 5 rows |
| row_count | INTEGER | Total rows |
| glue_job_run_id | TEXT | AWS Glue run ID |

### `chat_history`
| Column | Type | Description |
|---|---|---|
| id | SERIAL PK | - |
| user_id | INTEGER | Owner |
| question | TEXT | User question |
| answer | TEXT | LLM answer |
| file_name | TEXT | File queried |
| file_names | JSONB | Multiple files (multi-chat) |
| chat_type | TEXT | `single` or `multi` |
| generated_sql | TEXT | SQL used (structured) |
| file_type | TEXT | `structured`/`unstructured`/`multi` |
| result_rows | JSONB | SQL result rows |
| row_count | INTEGER | Number of rows returned |
| created_at | TIMESTAMPTZ | When asked |

---

## API Endpoints

### Auth
| Method | Endpoint | Description |
|---|---|---|
| POST | `/auth/register` | Create account `{ name, email, password }` |
| POST | `/auth/login` | Form login → JWT token |
| POST | `/auth/login-json` | JSON login `{ email, password }` → JWT token |

### Files
| Method | Endpoint | Description |
|---|---|---|
| POST | `/upload` | Upload single file (multipart/form-data) |
| POST | `/upload/batch` | Upload up to 20 files at once |
| POST | `/upload/{doc_id}/retry-queue` | Retry failed SQS send |
| GET | `/files` | List user's files with status + chunk count |
| DELETE | `/files/{file_name}` | Delete file from all locations |
| GET | `/structured/status/{file_name}` | Poll Glue job status |

### Chat
| Method | Endpoint | Description |
|---|---|---|
| POST | `/chat` | Ask a question (single or multi-file) |

```json
// Single file request
{
  "question": "What is the average salary?",
  "file_name": "employees.csv",
  "top_k": 5,
  "chat_history": []
}

// Multi-file request
{
  "question": "Compare revenue across all documents",
  "file_names": ["sales_q1.csv", "sales_q2.csv", "annual_report.pdf"],
  "top_k": 5
}
```

### History & Stats
| Method | Endpoint | Description |
|---|---|---|
| GET | `/history` | Recent chat history (optional `?file_name=`) |
| GET | `/stats` | File counts, chunk counts, vector count |
| GET | `/health` | API and DB health check |

### Internal (called by Lambda/EventBridge)
| Method | Endpoint | Description |
|---|---|---|
| POST | `/internal/dbt/run` | Run dbt build (requires X-Internal-Api-Key header) |
| POST | `/internal/structured/mark-ready` | Mark Glue job complete |

---

## AWS Services Used

| Service | Purpose | Free Tier |
|---|---|---|
| **S3** | Store uploaded files per user | 5GB free |
| **SQS** | Queue files for async processing | 1M requests/month free |
| **Lambda** | Route S3 events by file type | 1M invocations/month free |
| **Step Functions** | Orchestrate Glue + ECS workflows | 4,000 state transitions/month free |
| **Glue** | ETL: CSV/XLSX → RDS table | Pay per use (~$0.44/DPU-hr) |
| **ECS Fargate** | Run unstructured ingestion container | Pay per use |
| **RDS PostgreSQL** | Primary database + pgvector | db.t3.micro free for 12 months |
| **EventBridge** | Schedule hourly dbt runs | 14M events/month free |
| **CloudWatch** | Logs, alarms, dashboards | 5GB logs/month free |
| **X-Ray** | Request tracing and performance profiling | 1M traces/month free |

---

## Data Pipeline Flow

### Unstructured Files (PDF, DOCX, TXT, MD, PPTX)

```
1. User uploads file via POST /upload
2. FastAPI saves to S3: uploads/user_{id}/unstructured/{file}
3. FastAPI inserts into app_documents + file_upload_events
4. FastAPI sends SQS message
5. Lambda reads SQS → classifies as unstructured → triggers Step Functions
6. Step Functions starts ECS Fargate task with env vars
7. ECS worker:
   a. Downloads file from S3
   b. Extracts text (pdfplumber / python-docx / etc.)
   c. Splits into chunks (500 chars, 50 overlap)
   d. Stores chunks in raw_chunks table
   e. Generates embeddings via sentence-transformers (all-MiniLM-L6-v2)
   f. Stores 384-dim vectors in document_embeddings (pgvector)
   g. Updates processing_status = 'ready'
8. EventBridge runs dbt every hour:
   raw_chunks → stg_raw_chunks → mart_processed_chunks
```

### Structured Files (CSV, XLSX, JSON)

```
1. User uploads file via POST /upload
2. XLSX files are converted to CSV before S3 upload
3. FastAPI saves to S3: uploads/user_{id}/structured/{file}
4. FastAPI inserts into app_documents + structured_datasets + file_upload_events
5. FastAPI sends SQS message
6. Lambda reads SQS → classifies as structured → triggers Step Functions
7. Step Functions starts AWS Glue job with args:
   --S3_INPUT_PATH, --TABLE_NAME, --USER_ID, --DOCUMENT_ID, --FILE_NAME
8. Glue job:
   a. Reads CSV from S3 using PySpark
   b. Cleans column names (lowercase, no special chars)
   c. Trims strings, drops all-null rows, deduplicates
   d. Adds metadata: user_id, document_id, source_file_name, processed_at
   e. Creates unique table: u{user_id}_d{document_id}_{file_stem}
   f. Writes to RDS via JDBC
   g. Updates structured_datasets: schema_json, sample_json, status='ready'
9. Lambda (run_dbt_after_glue) fires on Glue SUCCEEDED event:
   a. Calls POST /internal/structured/mark-ready
   b. Calls POST /internal/dbt/run
```

### Chat (Unstructured)

```
1. POST /chat with { question, file_name }
2. FastAPI detects file_type = 'unstructured'
3. Embeds question using cached sentence-transformer
4. Queries pgvector: cosine similarity, top_k results, filtered by user_id
5. Builds context string from top chunks
6. Calls Groq (llama-3.3-70b) with context + question + chat history
7. Saves to chat_history
8. Returns answer + chunks + sources
```

### Chat (Structured / NL-to-SQL)

```
1. POST /chat with { question, file_name }
2. FastAPI detects file_type = 'structured'
3. Checks query_cache (MD5 hash) — returns cached if hit
4. Gets table_name from structured_datasets
5. Gets schema from cache (1hr) or DB
6. Gets column samples via single batch SQL query (array_agg)
7. Classifies question complexity (simple/standard/complex)
8. LLM generates SQL (llama-3.3-70b, temp=0)
9. Validates SQL (blocks unsafe keywords, wrong table, no user_id filter)
10. Executes SQL against RDS
11. On failure: LLM repairs SQL with error message, retries
12. If 0 rows: returns "No records found" (no LLM call)
13. LLM summarises result in plain English
14. Saves to chat_history with generated_sql, rows, columns
15. Caches result in query_cache
16. Returns answer + sql + table_name + row_count + rows
```

---

## Setup & Installation

### Prerequisites

- Python 3.11+
- PostgreSQL 15+ with pgvector extension
- AWS account with S3, SQS, Lambda, Glue, ECS configured
- Groq API key (free at console.groq.com)
- Docker (optional, for containerised local dev)

### Clone the repository

```bash
git clone https://github.com/patilyogesh22/aws_chatbot.git
cd aws_chatbot
```

### Install dependencies

```bash
# Create virtual environment
python -m venv myvenv
source myvenv/bin/activate    # Linux/Mac
myvenv\Scripts\activate       # Windows

# Install all packages
pip install -r requirements-core.txt
pip install -r requirements-ml.txt
```

---

## Environment Variables

Create a `.env` file in the root directory:

```env
# ── PostgreSQL / RDS ──────────────────────────────────────────
PG_HOST=your-rds-endpoint.eu-north-1.rds.amazonaws.com
PG_PORT=5432
PG_DB=docchat
PG_USER=postgres
PG_PASSWORD=your-db-password
PG_SCHEMA=public

# ── AWS ───────────────────────────────────────────────────────
AWS_REGION=eu-north-1
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
S3_BUCKET=your-s3-bucket-name

# ── SQS ───────────────────────────────────────────────────────
SQS_QUEUE_URL=https://sqs.eu-north-1.amazonaws.com/123456789/docchat-queue

# ── AI / LLM ──────────────────────────────────────────────────
GROQ_API_KEY=gsk_your-groq-key
GROQ_MODEL=llama-3.3-70b-versatile
GEMINI_API_KEY=your-gemini-key
GEMINI_MODEL=gemini-1.5-flash

# ── Auth ──────────────────────────────────────────────────────
JWT_SECRET_KEY=your-random-32-char-secret-key
JWT_ALGORITHM=HS256
JWT_EXPIRE_MINUTES=1440

# ── Embedding ─────────────────────────────────────────────────
EMBEDDING_MODEL=all-MiniLM-L6-v2
EMBEDDING_DIM=384

# ── Chunking ──────────────────────────────────────────────────
CHUNK_SIZE=500
CHUNK_OVERLAP=50
TOP_K=5

# ── ECS ───────────────────────────────────────────────────────
ECS_CLUSTER=docchat-cluster
ECS_TASK_DEFINITION=docchat-ingestion
ECS_SUBNETS=subnet-xxx,subnet-yyy
ECS_SECURITY_GROUPS=sg-xxx

# ── dbt ───────────────────────────────────────────────────────
RUN_DBT=false

# ── Internal API ──────────────────────────────────────────────
INTERNAL_API_KEY=your-internal-key

# ── Monitoring ────────────────────────────────────────────────
CLOUDWATCH_ENABLED=true
XRAY_ENABLED=true
```

---

## Running Locally

### Step 1 — Run RDS migrations

```bash
# Make sure PG_HOST points to your RDS instance
python migrations/run_migrations.py
```

### Step 2 — Start the API

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### Step 3 — Access the app

```
http://localhost:8000          → Frontend (index.html)
http://localhost:8000/docs     → Swagger API docs
http://localhost:8000/health   → Health check
```

---

## Running with Docker

```bash
cd docker

# Build and start
docker compose up --build

# Run in background
docker compose up -d --build

# View logs
docker compose logs -f api

# Stop
docker compose down
```

The API will be available at `http://localhost:8000`.

---

## Deploying to AWS EC2

### Step 1 — Launch EC2 instance

- AMI: Amazon Linux 2023 or Ubuntu 22.04
- Instance type: t3.medium (minimum for ML model loading)
- Security group: allow port 8000 inbound (or 80/443 with CloudFront)

### Step 2 — Install Docker on EC2

```bash
sudo yum update -y
sudo yum install docker -y
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker ec2-user
```

### Step 3 — Clone and configure

```bash
git clone https://github.com/patilyogesh22/aws_chatbot.git
cd aws_chatbot
cp .env.example .env
# Edit .env with your production values
nano .env
```

### Step 4 — Build and run

```bash
cd docker
docker compose up -d --build
```

### Step 5 — Run migrations

```bash
docker exec api python migrations/run_migrations.py
```

### Step 6 — (Optional) Install X-Ray daemon

```bash
wget https://s3.us-east-2.amazonaws.com/aws-xray-assets.us-east-2/xray-daemon/aws-xray-daemon-linux-3.x.zip
unzip aws-xray-daemon-linux-3.x.zip
chmod +x xray
sudo ./xray --bind 127.0.0.1:2000 &
```

---

## dbt Setup

dbt transforms `raw_chunks` → `stg_raw_chunks` → `mart_processed_chunks`.

### Local setup

```bash
# Install dbt
pip install dbt-core dbt-postgres

# Create profiles
mkdir -p ~/.dbt
cat > ~/.dbt/profiles.yml << EOF
dbt_chatbot:
  target: dev
  outputs:
    dev:
      type: postgres
      host: YOUR_PG_HOST
      user: YOUR_PG_USER
      password: YOUR_PG_PASSWORD
      port: 5432
      dbname: docchat
      schema: public
      threads: 4
EOF

# Test connection
cd dbt_chatbot
dbt debug

# Run only unstructured models
dbt build --select tag:unstructured
```

### Scheduled via EventBridge

```
EventBridge Rule → every 1 hour
  → Lambda → POST /internal/dbt/run
  → dbt build --select tag:unstructured
```

This replaces running dbt per file (which was slow). dbt now transforms all 
pending chunks in one batch every hour instead of per upload.

---

## Migrations

Run migrations in order:

```bash
python migrations/run_migrations.py
```

Or manually in order:

```bash
psql $DATABASE_URL -f migrations/001_auth_tables.sql
psql $DATABASE_URL -f migrations/002_raw_chunks.sql
psql $DATABASE_URL -f migrations/003_structured_datasets.sql
psql $DATABASE_URL -f migrations/004_file_upload_event.sql
psql $DATABASE_URL -f migrations/005_cleanup_crawler_columns.sql
psql $DATABASE_URL -f migrations/006_chat_history_sql.sql
psql $DATABASE_URL -f migrations/007_unique_structured_dataset.sql
psql $DATABASE_URL -f migrations/008_file_processing_status.sql
psql $DATABASE_URL -f migrations/009_multi_file_chat.sql
```

### Additional SQL to run on RDS

```sql
-- Add columns if using updated glue_job.py
ALTER TABLE structured_datasets ADD COLUMN IF NOT EXISTS schema_json  JSONB;
ALTER TABLE structured_datasets ADD COLUMN IF NOT EXISTS sample_json  JSONB;
ALTER TABLE structured_datasets ADD COLUMN IF NOT EXISTS row_count    INTEGER;
ALTER TABLE structured_datasets ADD COLUMN IF NOT EXISTS updated_at   TIMESTAMPTZ DEFAULT NOW();

-- Switch from IVFFlat to HNSW (faster vector search)
DROP INDEX IF EXISTS idx_document_embeddings_vector;
CREATE INDEX IF NOT EXISTS idx_document_embeddings_vector_hnsw
ON document_embeddings
USING hnsw (embedding vector_cosine_ops)
WITH (m = 16, ef_construction = 64);
```

---

## Frontend

The frontend is a single-page application built with vanilla HTML, CSS, and JavaScript — no framework, no build step.

**Files:**
- `frontend/index.html` — App shell, auth modals, landing page
- `frontend/style.css` — Full design system with dark/light mode, CSS variables
- `frontend/app.js` — All API calls, state management, UI updates

**Key features:**
- Landing page with feature cards and How it works section
- Auth modals (Sign In / Sign Up) with progress bar
- Session persistence via localStorage (1hr idle timeout)
- Profile dropdown with theme toggle and logout
- Sidebar as slide-in drawer (all screen sizes)
- File cards with Glue status badge polling every 8 seconds
- File click → auto-loads that file's chat history
- Multi-file selection for cross-document questions
- Structured query details panel (SQL, table name, row count)

**Served by FastAPI:**
```python
# app/main.py
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

app.mount("/static", StaticFiles(directory="frontend"), name="static")

@app.get("/")
def serve_frontend():
    return FileResponse("frontend/index.html")
```

---

## Author

**Yogesh Patil**
- GitHub: [@patilyogesh22](https://github.com/patilyogesh22)
- Project: [aws_chatbot](https://github.com/patilyogesh22/aws_chatbot)

---

*DocChat v3.5.0 — RAG + NL-to-SQL on AWS*