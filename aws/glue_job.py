"""
glue_job.py
AWS Glue Python Shell Job — replaces dbt for the AWS deployment.
Reads raw_chunks from S3 (Parquet/JSON), transforms them,
and writes mart_processed_chunks back to S3.

AWS Free Tier: Glue has 1M DPU-seconds/month free for the first year.
Schedule this job via EventBridge or trigger it from Lambda.
"""
import sys
import json
import boto3
from datetime import datetime, timezone

# ── Glue job args ─────────────────────────────────────────────────────────────
# When running locally for testing, these are set as env vars.
import os
S3_BUCKET      = os.getenv("S3_BUCKET", "your-chatbot-bucket")
RAW_PREFIX     = os.getenv("RAW_PREFIX",  "data/raw_chunks/")
MART_PREFIX    = os.getenv("MART_PREFIX", "data/mart_chunks/")
AWS_REGION     = os.getenv("AWS_REGION",  "ap-south-1")


def transform_chunks(raw_records: list) -> list:
    """
    Apply the same transformations as the dbt mart model:
    - Filter short chunks
    - Deduplicate by (file_name, chunk_index)
    - Add quality_tier and processed_at
    """
    seen   = {}
    result = []

    for rec in raw_records:
        text = (rec.get("chunk_text") or "").strip()

        # Filter noise
        if len(text) < 50:
            continue

        key = (rec["file_name"], rec["chunk_index"])
        # Keep latest ingestion
        if key not in seen or rec["ingested_at"] > seen[key]["ingested_at"]:
            word_count = len(text.split())
            seen[key]  = {
                **rec,
                "chunk_text":   text,
                "word_count":   word_count,
                "char_count":   len(text),
                "quality_tier": (
                    "rich"   if word_count >= 100 else
                    "normal" if word_count >= 30  else
                    "short"
                ),
                "processed_at": datetime.now(timezone.utc).isoformat(),
            }

    return list(seen.values())


def run():
    s3 = boto3.client("s3", region_name=AWS_REGION)

    # 1. Read raw chunks from S3
    raw_records = []
    paginator   = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=RAW_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".json"):
                continue
            body        = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
            raw_records += json.loads(body)

    print(f"[glue] Read {len(raw_records)} raw chunks from S3")

    if not raw_records:
        print("[glue] No raw chunks found — nothing to do.")
        return

    # 2. Transform
    mart_records = transform_chunks(raw_records)
    print(f"[glue] {len(mart_records)} chunks after transformation")

    # 3. Write mart chunks back to S3
    output_key = f"{MART_PREFIX}mart_processed_chunks.json"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=output_key,
        Body=json.dumps(mart_records, ensure_ascii=False),
        ContentType="application/json",
    )
    print(f"[glue] Wrote mart data → s3://{S3_BUCKET}/{output_key}")


if __name__ == "__main__":
    run()