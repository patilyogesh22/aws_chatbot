import json
import os
import urllib.request

API_URL = os.getenv("API_URL", "").rstrip("/")
INTERNAL_API_KEY = os.getenv("INTERNAL_API_KEY")


def lambda_handler(event, context):
    print("Glue event received:")
    print(json.dumps(event))

    detail = event.get("detail", {})
    state = detail.get("state")

    if state != "SUCCEEDED":
        return {
            "statusCode": 200,
            "body": "Glue job not succeeded. Skipping dbt."
        }

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

    with urllib.request.urlopen(req, timeout=120) as response:
        body = response.read().decode("utf-8")

    print("dbt response:", body)

    return {
        "statusCode": 200,
        "body": body
    }