import json
import os
import re
from pathlib import Path

import boto3
import psycopg2

# -------------------------
# AWS CLIENTS
# -------------------------
glue = boto3.client("glue")

# -------------------------
# ENV VARIABLES
# -------------------------
PG_HOST = os.getenv("PG_HOST")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_PORT = os.getenv("PG_PORT", "5432")

CRAWLER_NAME = os.getenv("CRAWLER_NAME", "chatbot-crawler")


# -------------------------
# DB CONNECTION
# -------------------------
def get_conn():
    return psycopg2.connect(
        host=PG_HOST,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        port=PG_PORT
    )


# -------------------------
# EXTRACT USER ID
# uploads/user_1/structured/dataset/file.csv
# -------------------------
def extract_user_id(key: str):
    match = re.search(r"uploads/(structured|unstructured)/user_(\d+)/", key)
    return int(match.group(2)) if match else None


# -------------------------
# FILE HELPERS
# -------------------------
def classify_file(file_name: str):
    ext = Path(file_name).suffix.lower()

    if ext in {".csv", ".json", ".parquet"}:
        return "structured"

    if ext in {".pdf", ".docx", ".txt", ".md", ".pptx"}:
        return "unstructured"

    return "unknown"


def get_dataset_name_from_key(key: str):
    """
    Example:
    uploads/user_2/structured/sales_data_50_records/sales_data_50_records.csv
    returns:
    sales_data_50_records
    """
    parts = key.split("/")

    if "structured" in parts:
        idx = parts.index("structured")
        if len(parts) > idx + 2:
            return parts[idx + 2]

    return Path(key).stem


# -------------------------
# FIND DOCUMENT ID
# -------------------------
def get_document_id(user_id, s3_key):
    if not user_id:
        return 0

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id
                    FROM app_documents
                    WHERE user_id = %s
                      AND s3_key = %s
                    ORDER BY id DESC
                    LIMIT 1
                """, (user_id, s3_key))

                row = cur.fetchone()
                return row[0] if row else 0

    except Exception as e:
        print("Could not fetch document_id:", str(e))
        return 0


# -------------------------
# STORE UPLOAD EVENT
# -------------------------
def store_upload_event(
    user_id,
    file_name,
    key,
    bucket,
    size,
    file_type,
    document_id,
    dataset_name
):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO file_upload_events
                (
                    user_id,
                    file_name,
                    s3_key,
                    bucket_name,
                    file_size,
                    file_type,
                    document_id,
                    dataset_name,
                    status
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                user_id,
                file_name,
                key,
                bucket,
                size,
                file_type,
                document_id,
                dataset_name,
                "crawler_started" if file_type == "structured" else "uploaded"
            ))

        conn.commit()


# -------------------------
# START GLUE CRAWLER
# -------------------------
def start_glue_crawler():
    try:
        print("Checking crawler...")

        crawler = glue.get_crawler(Name=CRAWLER_NAME)
        state = crawler["Crawler"]["State"]

        print("Crawler state:", state)

        if state == "READY":
            glue.start_crawler(Name=CRAWLER_NAME)
            print("Crawler started")
            return True

        print("Crawler already running, skipping start")
        return False

    except glue.exceptions.CrawlerRunningException:
        print("Crawler already running")
        return False

    except Exception as e:
        print("Glue crawler error:", str(e))
        raise e


# -------------------------
# LAMBDA HANDLER
# -------------------------
def lambda_handler(event, context):

    print("🚀 EVENT RECEIVED")
    print(json.dumps(event))

    results = []
    should_start_crawler = False

    try:
        for record in event["Records"]:
            bucket = record["s3"]["bucket"]["name"]
            key = record["s3"]["object"]["key"]
            size = record["s3"]["object"].get("size", 0)

            file_name = key.split("/")[-1]
            user_id = extract_user_id(key)
            file_type = classify_file(file_name)
            document_id = get_document_id(user_id, key)
            dataset_name = get_dataset_name_from_key(key)

            print(f"📄 File: {file_name}")
            print(f"👤 User ID: {user_id}")
            print(f"📂 File type: {file_type}")
            print(f"📊 Dataset: {dataset_name}")
            print(f"🧾 Document ID: {document_id}")
            print(f"🔑 S3 Key: {key}")

            if file_type == "structured":
                should_start_crawler = True

            store_upload_event(
                user_id=user_id,
                file_name=file_name,
                key=key,
                bucket=bucket,
                size=size,
                file_type=file_type,
                document_id=document_id,
                dataset_name=dataset_name
            )

            results.append({
                "file_name": file_name,
                "file_type": file_type,
                "dataset_name": dataset_name,
                "user_id": user_id,
                "document_id": document_id
            })

        if should_start_crawler:
            print("👉 Structured file detected. Starting Glue crawler...")
            crawler_started = start_glue_crawler()
        else:
            print("ℹ️ No structured file detected. Glue crawler skipped.")
            crawler_started = False

        print("✅ Lambda completed")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "status": "success",
                "crawler_started": crawler_started,
                "results": results
            })
        }

    except Exception as e:
        print("❌ Lambda error:", str(e))

        return {
            "statusCode": 500,
            "body": str(e)
        }