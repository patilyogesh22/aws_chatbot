import json
import os

import boto3


AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")

sqs_client = boto3.client(
    "sqs",
    region_name=AWS_REGION,
)


def send_file_to_queue(
    *,
    user_id: int,
    document_id: int,
    file_name: str,
    file_type: str,
    s3_bucket: str,
    s3_key: str,
    file_size: int,
    dataset_name: str | None = None,
    table_name: str | None = None,
) -> str:
    """Send one file-processing message to Amazon SQS."""

    if not SQS_QUEUE_URL:
        raise RuntimeError("SQS_QUEUE_URL is not configured")

    if file_type not in {"structured", "unstructured"}:
        raise ValueError(
            "file_type must be 'structured' or 'unstructured'"
        )

    if file_type == "structured":
        if not dataset_name:
            raise ValueError(
                "dataset_name is required for structured files"
            )

        if not table_name:
            raise ValueError(
                "table_name is required for structured files"
            )
    else:
        dataset_name = None
        table_name = None

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

    message_id = response.get("MessageId")

    if not message_id:
        raise RuntimeError("SQS did not return a MessageId")

    return message_id