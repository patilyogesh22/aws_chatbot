import json
import os
import re
from pathlib import Path

import psycopg2
import boto3

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
# uploads/user_1/file.csv
# -------------------------
def extract_user_id(key: str):
    match = re.search(r"uploads/user_(\d+)/", key)

    if match:
        return int(match.group(1))

    return None


# -------------------------
# FILE CLASSIFIER
# -------------------------
def classify_file(file_name: str):
    ext = Path(file_name).suffix.lower()

    structured = {
        ".csv",
        ".xlsx",
        ".xls",
        ".json"
    }

    unstructured = {
        ".pdf",
        ".docx",
        ".txt",
        ".md",
        ".pptx"
    }

    if ext in structured:
        return "structured"

    if ext in unstructured:
        return "unstructured"

    return "unknown"


# -------------------------
# START GLUE CRAWLER
# -------------------------
def start_glue_crawler():
    try:
        print("Checking crawler...")

        crawler = glue.get_crawler(
            Name=CRAWLER_NAME
        )

        state = crawler["Crawler"]["State"]

        print("Crawler state:", state)

        if state == "READY":
            glue.start_crawler(
                Name=CRAWLER_NAME
            )

            print("Crawler started")
        else:
            print("Crawler already running, skipping start")

    except glue.exceptions.CrawlerRunningException:
        print("Crawler already running")

    except Exception as e:
        print("Glue error:", str(e))
        raise e


# -------------------------
# LAMBDA HANDLER
# -------------------------
def lambda_handler(event, context):

    print("🚀 EVENT RECEIVED")
    print(json.dumps(event))

    try:
        should_start_crawler = False

        for record in event["Records"]:

            bucket = record["s3"]["bucket"]["name"]
            key = record["s3"]["object"]["key"]
            size = record["s3"]["object"].get("size", 0)

            file_name = key.split("/")[-1]
            user_id = extract_user_id(key)
            file_type = classify_file(file_name)

            print(f"📄 Processing file: {file_name}")
            print(f"👤 User ID: {user_id}")
            print(f"📂 File type: {file_type}")
            print(f"🔑 S3 Key: {key}")

            # -------------------------
            # STORE METADATA
            # -------------------------
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
                            file_type
                        )
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        user_id,
                        file_name,
                        key,
                        bucket,
                        size,
                        file_type
                    ))

                conn.commit()

            print(f"✅ Stored metadata for {file_name}")

            # Start crawler only for structured files
            if file_type == "structured":
                should_start_crawler = True

        # -------------------------
        # START GLUE CRAWLER ONLY FOR STRUCTURED FILES
        # -------------------------
        if should_start_crawler:
            print("👉 Structured file detected. Starting Glue crawler...")
            start_glue_crawler()
        else:
            print("ℹ️ No structured file detected. Glue crawler skipped.")

        print("✅ Lambda completed")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "status": "success",
                "crawler_started": should_start_crawler
            })
        }

    except Exception as e:
        print("❌ Lambda error:", str(e))

        return {
            "statusCode": 500,
            "body": str(e)
        }