import json
import os
import re
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
# EXTRACT USER ID
# uploads/user_1/file.csv
# -------------------------
def extract_user_id(key: str):

    match = re.search(r"uploads/user_(\d+)/", key)

    if match:
        return int(match.group(1))

    return None


# -------------------------
# LAMBDA HANDLER
# -------------------------
def lambda_handler(event, context):

    print("🚀 EVENT RECEIVED")
    print(json.dumps(event))

    try:

        for record in event["Records"]:

            bucket = record["s3"]["bucket"]["name"]
            key = record["s3"]["object"]["key"]
            size = record["s3"]["object"].get("size", 0)

            file_name = key.split("/")[-1]

            user_id = extract_user_id(key)

            print(f"📄 Processing file: {file_name}")
            print(f"👤 User ID: {user_id}")

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
                            file_size
                        )
                        VALUES (%s,%s,%s,%s,%s)
                    """, (
                        user_id,
                        file_name,
                        key,
                        bucket,
                        size
                    ))

                conn.commit()

            print(f"✅ Stored metadata for {file_name}")

        # -------------------------
        # START GLUE CRAWLER
        # -------------------------
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

                print("Crawler already running")

        except glue.exceptions.CrawlerRunningException:

            print("Crawler already running")

        except Exception as e:

            print("Glue error:", str(e))
            raise e

        print("Lambda completed")

        return {
            "statusCode": 200,
            "body": json.dumps("success")
        }

    except Exception as e:

        print("Lambda error:", str(e))

        return {
            "statusCode": 500,
            "body": str(e)
        }