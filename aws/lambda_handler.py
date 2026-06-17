import json
import os
import psycopg2
import boto3

glue = boto3.client("glue")

PG_HOST = os.getenv("PG_HOST")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_PORT = os.getenv("PG_PORT", "5432")
CRAWLER_NAME = "chatbot-crawler"

def get_conn():
    return psycopg2.connect(
        host=PG_HOST,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        port=PG_PORT
    )

def lambda_handler(event, context):

    try:
        for record in event["Records"]:

            bucket = record["s3"]["bucket"]["name"]
            key = record["s3"]["object"]["key"]
            size = record["s3"]["object"].get("size", 0)

            file_name = key.split("/")[-1]

            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO file_upload_events
                        (file_name, s3_key, bucket_name, file_size)
                        VALUES (%s,%s,%s,%s)
                    """, (file_name, key, bucket, size))

                conn.commit()

            print(f"Stored metadata for {file_name}")

        # 👇 RUN CRAWLER ONCE AFTER ALL RECORDS
        try:
            print("Triggering crawler:", CRAWLER_NAME)
            response = glue.start_crawler(Name=CRAWLER_NAME)
            print("Crawler response:", response)

        except glue.exceptions.CrawlerRunningException:
            print("Crawler already running")

        except Exception as e:
            print("Crawler error:", str(e))
            raise e

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