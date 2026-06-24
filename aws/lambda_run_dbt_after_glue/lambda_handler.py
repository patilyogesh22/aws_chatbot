import json
import os
import urllib.request

import boto3


API_URL = os.getenv("API_URL", "").rstrip("/")
INTERNAL_API_KEY = os.getenv("INTERNAL_API_KEY")
GLUE_JOB_NAME = os.getenv("GLUE_JOB_NAME", "structured-file-etl-job")

glue = boto3.client(
    "glue",
    region_name=os.getenv("AWS_REGION", "eu-north-1")
)


def post_internal(url, payload):
    req = urllib.request.Request(
        url,
        method="POST",
        headers={
            "X-Internal-Api-Key": INTERNAL_API_KEY,
            "Content-Type": "application/json",
        },
        data=json.dumps(payload).encode("utf-8"),
    )

    with urllib.request.urlopen(req, timeout=120) as response:
        return response.read().decode("utf-8")


def lambda_handler(event, context):
    print("Glue event received:")
    print(json.dumps(event))

    detail = event.get("detail", {})

    job_name = detail.get("jobName")
    state = detail.get("state")
    job_run_id = detail.get("jobRunId")

    print("Job Name:", job_name)
    print("State:", state)
    print("Job Run ID:", job_run_id)

    if job_name != GLUE_JOB_NAME:
        print(f"Ignoring Glue job: {job_name}")
        return {
            "statusCode": 200,
            "body": json.dumps({
                "status": "ignored",
                "reason": f"Unexpected Glue job: {job_name}",
            }),
        }

    if state != "SUCCEEDED":
        print("Glue job not succeeded. Skipping.")
        return {
            "statusCode": 200,
            "body": json.dumps({
                "status": "skipped",
                "reason": f"Glue state={state}",
            }),
        }

    if not API_URL:
        raise Exception("API_URL environment variable not configured")

    if not INTERNAL_API_KEY:
        raise Exception("INTERNAL_API_KEY environment variable not configured")

    try:
        job_run = glue.get_job_run(
            JobName=job_name,
            RunId=job_run_id,
            PredecessorsIncluded=False,
        )

        glue_args = job_run["JobRun"].get("Arguments", {})

        print("Glue job arguments:")
        print(json.dumps(glue_args))

        user_id = int(glue_args.get("--USER_ID", 0))
        document_id = int(glue_args.get("--DOCUMENT_ID", 0))
        table_name = glue_args.get("--TABLE_NAME")
        file_name = glue_args.get("--FILE_NAME")

        if not user_id or not document_id or not table_name:
            raise Exception(
                f"Missing Glue args. user_id={user_id}, "
                f"document_id={document_id}, table_name={table_name}"
            )

        # 1. Mark structured dataset ready
        ready_url = f"{API_URL}/internal/structured/mark-ready"

        ready_payload = {
            "user_id": user_id,
            "document_id": document_id,
            "table_name": table_name,
            "status": "ready",
        }

        print("Calling mark-ready:", ready_url)
        print("Payload:", json.dumps(ready_payload))

        ready_response = post_internal(
            ready_url,
            ready_payload,
        )

        print("mark-ready response:", ready_response)

        # 2. Run dbt after Glue success
        dbt_url = f"{API_URL}/internal/dbt/run"

        print("Calling dbt:", dbt_url)

        dbt_response = post_internal(
            dbt_url,
            {},
        )

        print("dbt response:", dbt_response)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "status": "success",
                "glue_job_name": job_name,
                "glue_job_run_id": job_run_id,
                "user_id": user_id,
                "document_id": document_id,
                "file_name": file_name,
                "table_name": table_name,
                "mark_ready_response": ready_response,
                "dbt_response": dbt_response,
            }),
        }

    except Exception as e:
        print("Error in Glue success handler:", str(e))

        return {
            "statusCode": 500,
            "body": json.dumps({
                "status": "failed",
                "glue_job_name": job_name,
                "glue_job_run_id": job_run_id,
                "error": str(e),
            }),
        }