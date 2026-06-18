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
        uploads/user_1/

    Final key:
        uploads/user_1/<uuid>_file.csv
    """

    if not BUCKET:
        raise ValueError("S3_BUCKET environment variable not set")

    if not prefix.endswith("/"):
        prefix += "/"

    safe_filename = sanitize_filename(filename)
    key = f"{prefix}{uuid.uuid4()}_{safe_filename}"

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
    key = f"{prefix}{uuid.uuid4()}_{filename}"

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