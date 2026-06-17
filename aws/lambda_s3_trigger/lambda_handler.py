import json
import os
import psycopg2
import boto3

# -------------------------
# AWS CLIENT
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

CRAWLER_NAME = "chatbot-crawler"


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
# LAMBDA HANDLER
# -------------------------
def lambda_handler(event, context):

    print("🚀 EVENT RECEIVED:")
    print(json.dumps(event))

    try:

        # -------------------------
        # PROCESS S3 RECORDS
        # -------------------------
        for record in event["Records"]:

            bucket = record["s3"]["bucket"]["name"]
            key = record["s3"]["object"]["key"]
            size = record["s3"]["object"].get("size", 0)

            file_name = key.split("/")[-1]

            print(f"📄 Processing file: {file_name}")

            # -------------------------
            # INSERT INTO POSTGRES
            # -------------------------
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO file_upload_events
                        (file_name, s3_key, bucket_name, file_size)
                        VALUES (%s, %s, %s, %s)
                    """, (file_name, key, bucket, size))

                conn.commit()

            print(f"✅ Stored metadata for {file_name}")

        # -------------------------
        # START GLUE CRAWLER (ONCE)
        # -------------------------
        print("👉 BEFORE STARTING CRAWLER:", CRAWLER_NAME)

        try:
            response = glue.start_crawler(Name=CRAWLER_NAME)

            print("✅ CRAWLER STARTED SUCCESSFULLY")
            print("📦 RESPONSE:", json.dumps(response, default=str))

        except glue.exceptions.CrawlerRunningException:
            print("⚠️ CRAWLER already running, skipping start")

        except Exception as e:
            print("❌ CRAWLER ERROR:", str(e))
            raise e

        print("👉 AFTER CRAWLER CALL COMPLETED")

        # -------------------------
        # RESPONSE
        # -------------------------
        return {
            "statusCode": 200,
            "body": json.dumps("success")
        }

    except Exception as e:

        print("❌ LAMBDA ERROR:", str(e))

        return {
            "statusCode": 500,
            "body": str(e)
        }