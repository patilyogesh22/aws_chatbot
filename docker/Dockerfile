FROM python:3.11-slim

WORKDIR /app

# ─────────────────────────────────────────────
# System dependencies (minimal + required only)
# ─────────────────────────────────────────────
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libffi-dev \
    libssl-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# ─────────────────────────────────────────────
# Copy requirements first (for caching)
# ─────────────────────────────────────────────
ENV PIP_NO_BUILD_ISOLATION=1
RUN pip install --upgrade pip
RUN pip install torch --index-url https://download.pytorch.org/whl/cpu
COPY requirements-core.txt .
RUN pip install -r requirements-core.txt

COPY requirements-ml.txt .
RUN pip install -r requirements-ml.txt


# ─────────────────────────────────────────────
# Copy ONLY necessary project files
# (important for fast builds)
# ─────────────────────────────────────────────
COPY app/ app/
COPY aws/ aws/
COPY aws/profiles.yml /app/aws/profiles.yml
COPY dbt_chatbot/ dbt_chatbot/
COPY frontend/ frontend/
COPY *.py ./

# ─────────────────────────────────────────────
# Create required runtime directories
# ─────────────────────────────────────────────
RUN mkdir -p data/raw data/processed vector_store

# ─────────────────────────────────────────────
# IMPORTANT FIX:
# DO NOT download ML model during build ❌
# It makes build slow (you had this issue)
# ─────────────────────────────────────────────

# Model will load at runtime automatically

# ─────────────────────────────────────────────
# Expose FastAPI port
# ─────────────────────────────────────────────
EXPOSE 8000

# ─────────────────────────────────────────────
# Run app
# ─────────────────────────────────────────────
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]