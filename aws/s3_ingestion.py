import boto3
import os
import uuid

# ──────────────────────────────────────────
# ENV CONFIG (SAFE DEFAULTS)
# ──────────────────────────────────────────
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
BUCKET = os.getenv("S3_BUCKET", "")

if not BUCKET:
    print("[WARNING] S3_BUCKET is not set. S3 uploads will fail.")


# ──────────────────────────────────────────
# S3 CLIENT (lazy safe init)
# ──────────────────────────────────────────
def get_s3_client():
    return boto3.client("s3", region_name=AWS_REGION)


# ──────────────────────────────────────────
# Upload from FastAPI / Streamlit stream
# ──────────────────────────────────────────

def upload_fileobj_to_s3(file_obj, filename: str, prefix="uploads/"):
    """
    Upload directly from API without local storage
    """

    if not BUCKET:
        raise ValueError("S3_BUCKET environment variable not set")

    s3 = get_s3_client()
    key = f"{prefix}{uuid.uuid4()}_{filename}"

    s3.upload_fileobj(
        Fileobj=file_obj,
        Bucket=BUCKET,
        Key=key,
        ExtraArgs={
            "ContentType": "application/octet-stream"
        }
    )

    return {
        "s3_key": key,
        "original_filename": filename
    }


# ──────────────────────────────────────────
# Upload from local file (testing only)
# ──────────────────────────────────────────
def upload_file_to_s3(file_path: str, prefix="uploads/"):
    """
    Local testing utility only
    """

    if not BUCKET:
        raise ValueError("S3_BUCKET environment variable not set")

    s3 = get_s3_client()
    filename = os.path.basename(file_path)
    key = f"{prefix}{uuid.uuid4()}_{filename}"

    s3.upload_file(
        Filename=file_path,
        Bucket=BUCKET,
        Key=key
    )

    return key


# ──────────────────────────────────────────
# Generate S3 URL
# ──────────────────────────────────────────
def get_s3_url(key: str):
    return f"https://{BUCKET}.s3.{AWS_REGION}.amazonaws.com/{key}"