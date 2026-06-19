import boto3
import os
import uuid
import re
from urllib.parse import quote


AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
BUCKET = os.getenv("S3_BUCKET", "")

if not BUCKET:
    print("[WARNING] S3_BUCKET is not set. S3 uploads will fail.")


def get_s3_client():
    return boto3.client("s3", region_name=AWS_REGION)


def sanitize_filename(filename: str) -> str:
    """
    Make filename safe for S3 key.
    """
    filename = os.path.basename(filename)
    filename = filename.replace(" ", "_")
    filename = re.sub(r"[^A-Za-z0-9._-]", "_", filename)
    return filename

def upload_fileobj_to_s3(file_obj, filename: str, prefix: str = "uploads/"):
    """
    Upload file object directly to S3.

    Example prefix:
        uploads/user_1/unstructured/

    Final key:
        uploads/user_1/unstructured/Dbt_notes.pdf
    """

    if not BUCKET:
        raise ValueError("S3_BUCKET environment variable not set")

    if not prefix.endswith("/"):
        prefix += "/"

    safe_filename = sanitize_filename(filename)

    # No UUID because duplicate check is already done using file_hash + user_id
    key = f"{prefix}{safe_filename}"

    file_obj.seek(0)

    s3 = get_s3_client()

    s3.upload_fileobj(
        Fileobj=file_obj,
        Bucket=BUCKET,
        Key=key,
        ExtraArgs={
            "ContentType": "application/octet-stream"
        }
    )

    return {
        "bucket": BUCKET,
        "s3_key": key,
        "original_filename": safe_filename,
    }


def upload_file_to_s3(file_path: str, prefix: str = "uploads/"):
    """
    Local testing utility.
    """

    if not BUCKET:
        raise ValueError("S3_BUCKET environment variable not set")

    if not prefix.endswith("/"):
        prefix += "/"

    s3 = get_s3_client()

    filename = sanitize_filename(os.path.basename(file_path))
    key = f"{prefix}{filename}"

    s3.upload_file(
        Filename=file_path,
        Bucket=BUCKET,
        Key=key
    )

    return {
        "bucket": BUCKET,
        "s3_key": key,
        "original_filename": filename,
    }


def get_s3_url(key: str):
    """
    Generate public-style S3 URL.
    Note: this will only open in browser if object/bucket policy allows access.
    """
    encoded_key = quote(key)
    return f"https://{BUCKET}.s3.{AWS_REGION}.amazonaws.com/{encoded_key}"

def delete_s3_object(s3_key: str):
    """
    Delete file from S3 bucket using s3_key.
    """
    if not BUCKET:
        raise ValueError("S3_BUCKET environment variable not set")

    s3 = get_s3_client()

    s3.delete_object(
        Bucket=BUCKET,
        Key=s3_key
    )

    print(f"[s3] Deleted object: {s3_key}")