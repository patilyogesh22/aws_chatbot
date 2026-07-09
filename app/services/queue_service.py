import json
import os
import re
from pathlib import Path

import boto3


AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")

sqs_client = boto3.client("sqs", region_name=AWS_REGION)


def clean_table_name(name: str):
    name = Path(name).stem.lower()
    name = re.sub(r"[^a-z0-9_]", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_")


def send_file_to_queue(
    *,
    user_id: int,
    document_id: int,
    file_name: str,
    file_type: str,
    s3_bucket: str,
    s3_key: str,
    file_size: int,
):
    if not SQS_QUEUE_URL:
        raise RuntimeError("SQS_QUEUE_URL is not set")

    dataset_name = clean_table_name(file_name)
    table_name = f"u{user_id}_d{document_id}_{dataset_name}"

    message = {
        "user_id": user_id,
        "document_id": document_id,
        "file_name": file_name,
        "file_type": file_type,
        "bucket": s3_bucket,
        "s3_key": s3_key,
        "s3_path": f"s3://{s3_bucket}/{s3_key}",
        "file_size": file_size,
        "dataset_name": dataset_name,
        "table_name": table_name,
    }

    response = sqs_client.send_message(
        QueueUrl=SQS_QUEUE_URL,
        MessageBody=json.dumps(message),
    )

    return response["MessageId"]