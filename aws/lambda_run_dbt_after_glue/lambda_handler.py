import json
import os
import urllib.request

API_URL = os.getenv("API_URL")
INTERNAL_API_KEY = os.getenv("INTERNAL_API_KEY")


def lambda_handler(event, context):

    print(json.dumps(event))

    detail = event.get("detail", {})

    state = detail.get("state")

    if state != "SUCCEEDED":
        return {
            "statusCode": 200,
            "body": "Glue not successful"
        }

    url = f"{API_URL}/internal/dbt/run"

    req = urllib.request.Request(
        url,
        method="POST",
        headers={
            "X-Internal-Api-Key": INTERNAL_API_KEY,
            "Content-Type": "application/json"
        },
        data=b"{}"
    )

    response = urllib.request.urlopen(req)

    return {
        "statusCode": 200,
        "body": response.read().decode()
    }