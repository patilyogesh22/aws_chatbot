# RAG Chatbot — dbt + Groq + AWS

Upload any file (PDF, DOCX, CSV, TXT, XLSX, JSON, PPTX, MD) and chat with it using Groq LLM and vector search. Built with FastAPI, Streamlit, dbt, PostgreSQL + pgvector, and AWS free-tier services.

---

## Architecture

```
Local:                            AWS (Free Tier):
──────                            ────────────────
User → Streamlit                  User → Streamlit (EC2 t2.micro)
         ↓                                  ↓
      FastAPI                         API Gateway HTTP API
         ↓                                  ↓
   dbt (transform)                   Lambda (main API)
         ↓                                  ↓
   ChromaDB (vectors)             RDS PostgreSQL + pgvector
   PostgreSQL (chunks)                       ↑
                                     S3 upload → Lambda (ingestion)
                                             → Glue job (transform)
```

---

## Folder Structure

```
rag-chatbot/
├── app/                    # FastAPI backend (local)
│   ├── main.py
│   ├── ingestion.py
│   ├── embeddings.py
│   ├── retriever.py
│   ├── llm.py
│   └── config.py
├── dbt_project/            # dbt models (local transform)
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/stg_raw_chunks.sql
│       └── marts/mart_processed_chunks.sql
├── frontend/
│   └── streamlit_app.py    # ← improved UI (v3)
├── aws/
│   ├── lambda_handler.py   # Main API Lambda
│   ├── s3_ingestion.py     # S3-triggered ingestion Lambda
│   ├── glue_job.py         # AWS Glue (replaces dbt)
│   ├── cloudformation.yml  # All AWS infrastructure
│   └── deploy.sh           # One-command deploy
├── docker/
│   ├── Dockerfile
│   ├── docker-compose.yml
│   └── init.sql            # pgvector schema
├── requirements.txt
├── .env.example
└── README.md
```

---

## Local Setup

### 1. Prerequisites
- Python 3.11+
- Docker + Docker Compose
- Groq API key → https://console.groq.com

### 2. Clone & install
```bash
git clone <your-repo>
cd rag-chatbot
cp .env.example .env          # fill in GROQ_API_KEY
pip install -r requirements.txt
```

### 3. Start PostgreSQL with pgvector
```bash
docker compose -f docker/docker-compose.yml up postgres -d
```

### 4. Run dbt to create/migrate tables
```bash
cd dbt_project
dbt run --profiles-dir .
cd ..
```

### 5. Start the API
```bash
uvicorn app.main:app --reload --port 8000
```

### 6. Start Streamlit
```bash
streamlit run frontend/streamlit_app.py
```

Open http://localhost:8501 — upload a file and start chatting.

---

## AWS Deployment (Free Tier)

### Free-tier resources used
| Service | Free Tier Limit | Usage |
|---|---|---|
| Lambda | 1M req/mo, 400K GB-s | API + ingestion |
| API Gateway (HTTP) | 1M req/mo (12 months) | REST API |
| S3 | 5 GB (12 months) | Document storage |
| RDS PostgreSQL | db.t3.micro, 20 GB (12 months) | Chunks + vectors |
| Glue (pythonshell) | 1M DPU-s/mo | ETL (replaces dbt) |
| SSM Parameter Store | 10K req/mo | Secrets |

### Step 1: Prerequisites
```bash
# Install AWS CLI
pip install awscli
aws configure          # set Access Key, Secret, region

# Verify
aws sts get-caller-identity
```

### Step 2: Create a deploy artifacts bucket (one-time)
```bash
aws s3 mb s3://my-rag-deploy-artifacts --region us-east-1
```

### Step 3: Deploy everything
```bash
export DEPLOY_BUCKET=my-rag-deploy-artifacts
export DB_PASSWORD="YourStrongPassword123!"
export GROQ_API_KEY="gsk_xxxxxxxxxxxx"

chmod +x aws/deploy.sh
./aws/deploy.sh
```

The script will:
1. Build and zip Lambda packages with all dependencies
2. Upload zips + Glue script to S3
3. Deploy CloudFormation stack (VPC, RDS, Lambda, API Gateway, Glue)
4. Print the API endpoint URL

### Step 4: Initialize pgvector on RDS
Run once after first deploy (the deploy script prints the command):
```bash
psql -h <rds-endpoint> -U ragadmin -d ragchatbot \
  -c "CREATE EXTENSION IF NOT EXISTS vector;"
```

### Step 5: Connect Streamlit to AWS API
```bash
export API_URL=https://xxxxxxxx.execute-api.us-east-1.amazonaws.com
streamlit run frontend/streamlit_app.py
```

### Step 6 (Optional): Deploy Streamlit to EC2 Free Tier
```bash
# Launch t2.micro EC2 (Amazon Linux 2023, free tier)
# SSH in, then:
sudo yum install python3-pip git -y
git clone <your-repo>
cd rag-chatbot
pip3 install streamlit requests
echo "API_URL=https://your-api-gateway-url" > .env

# Run with systemd or screen
screen -S streamlit
API_URL=https://your-api-gateway-url streamlit run frontend/streamlit_app.py \
  --server.port 8501 --server.address 0.0.0.0
```
Open port 8501 in your EC2 security group and visit http://<ec2-public-ip>:8501

---

## How the Pipeline Works

### Local (dbt)
1. **Upload** → FastAPI saves file to `data/raw/`
2. **Ingest** → `ingestion.py` parses file → writes `raw_chunks` to PostgreSQL
3. **Transform** → `dbt run` cleans text → `mart_processed_chunks`
4. **Embed** → `embeddings.py` generates sentence-transformer embeddings → stores in ChromaDB
5. **Chat** → question embedded → ChromaDB similarity search → top-K chunks → Groq LLM → answer

### AWS (Glue replaces dbt)
1. **Upload** → Streamlit POST → API Gateway → Lambda → S3
2. **Ingest** → S3 PutObject event → `s3_ingestion` Lambda → writes `raw_chunks` to RDS
3. **Transform** → `/dbt/run` → Lambda starts Glue job → Glue cleans + embeds → `processed_chunks` in RDS
4. **Chat** → Lambda → pgvector cosine search → Groq LLM → answer

---

## Environment Variables

| Variable | Description | Required |
|---|---|---|
| `GROQ_API_KEY` | Groq API key | ✅ |
| `PG_HOST` | PostgreSQL host | ✅ |
| `PG_DB` | Database name | ✅ |
| `PG_USER` | DB username | ✅ |
| `PG_PASSWORD` | DB password | ✅ |
| `API_URL` | FastAPI/Lambda base URL | ✅ (Streamlit) |
| `S3_BUCKET` | Docs S3 bucket (AWS only) | AWS only |
| `GLUE_JOB_NAME` | Glue job name (AWS only) | AWS only |
| `AWS_REGION` | AWS region | Deploy only |
| `DEPLOY_BUCKET` | S3 bucket for Lambda zips | Deploy only |

---

## Troubleshooting

**Lambda cold start timeout**: increase Lambda timeout in CloudFormation `Timeout: 60` → `90`

**pgvector not found**: run `CREATE EXTENSION IF NOT EXISTS vector;` on RDS

**Glue job fails**: check CloudWatch Logs → `/aws-glue/python-jobs/output`

**S3 upload not triggering Lambda**: verify the S3 notification is on `uploads/` prefix and the Lambda permission's `SourceAccount` matches your account ID