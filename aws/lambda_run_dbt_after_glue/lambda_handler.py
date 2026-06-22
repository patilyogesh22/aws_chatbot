import json
import os
import urllib.request


API_URL = os.getenv("API_URL", "").rstrip("/")
INTERNAL_API_KEY = os.getenv("INTERNAL_API_KEY")
GLUE_JOB_NAME = os.getenv(
    "GLUE_JOB_NAME",
    "structured-file-etl-job"
)


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

    # Only react to the structured ETL job
    if job_name != GLUE_JOB_NAME:
        print(f"Ignoring Glue job: {job_name}")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "status": "ignored",
                "reason": f"Unexpected Glue job: {job_name}"
            })
        }

    # Only run dbt when Glue succeeds
    if state != "SUCCEEDED":
        print("Glue job not succeeded. Skipping dbt.")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "status": "skipped",
                "reason": f"Glue state={state}"
            })
        }

    if not API_URL:
        raise Exception("API_URL environment variable not configured")

    if not INTERNAL_API_KEY:
        raise Exception("INTERNAL_API_KEY environment variable not configured")

    url = f"{API_URL}/internal/dbt/run"

    print("Calling URL:", url)

    req = urllib.request.Request(
        url,
        method="POST",
        headers={
            "X-Internal-Api-Key": INTERNAL_API_KEY,
            "Content-Type": "application/json",
        },
        data=b"{}",
    )

    try:
        with urllib.request.urlopen(req, timeout=120) as response:
            body = response.read().decode("utf-8")

        print("dbt response:", body)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "status": "success",
                "glue_job_name": job_name,
                "glue_job_run_id": job_run_id,
                "dbt_response": body,
            })
        }

    except Exception as e:
        print("Error calling dbt endpoint:", str(e))

        return {
            "statusCode": 500,
            "body": json.dumps({
                "status": "failed",
                "glue_job_name": job_name,
                "glue_job_run_id": job_run_id,
                "error": str(e),
            })
        }