import os
import boto3

AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
METRIC_NAMESPACE = os.getenv("CLOUDWATCH_METRIC_NAMESPACE", "DocChat")

cloudwatch = boto3.client("cloudwatch", region_name=AWS_REGION)


def send_metric(
    metric_name: str,
    value: float = 1,
    unit: str = "Count",
    dimensions: list | None = None,
):
    """
    Send custom metric to CloudWatch.
    This should never break the main app if CloudWatch fails.
    """
    try:
        metric_data = {
            "MetricName": metric_name,
            "Value": value,
            "Unit": unit,
        }

        if dimensions:
            metric_data["Dimensions"] = dimensions

        cloudwatch.put_metric_data(
            Namespace=METRIC_NAMESPACE,
            MetricData=[metric_data],
        )

    except Exception as e:
        print(f"[cloudwatch-metric] Failed to send {metric_name}: {e}")