FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libffi-dev \
    libssl-dev \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements-core.txt .
COPY requirements-ml.txt .

RUN pip install --no-cache-dir --upgrade pip

RUN pip install --no-cache-dir -r requirements-core.txt
RUN pip install --no-cache-dir -r requirements-ml.txt

COPY app ./app
COPY aws ./aws
COPY dbt_chatbot ./dbt_chatbot
COPY migrations ./migrations

RUN mkdir -p \
    data/raw \
    data/processed \
    vector_store \
    logs

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
CMD curl -f http://localhost:8000/health || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]