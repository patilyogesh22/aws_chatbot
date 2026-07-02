import time
import boto3

AWS_REGION = "eu-north-1"

textract = boto3.client("textract", region_name=AWS_REGION)


def extract_text_with_textract(bucket: str, key: str) -> str:
    print("[textract] Starting Textract OCR job...")
    print("[textract] Bucket:", bucket)
    print("[textract] Key:", key)

    response = textract.start_document_text_detection(
        DocumentLocation={
            "S3Object": {
                "Bucket": bucket,
                "Name": key,
            }
        }
    )

    job_id = response["JobId"]
    print("[textract] JobId:", job_id)

    while True:
        result = textract.get_document_text_detection(JobId=job_id)
        status = result["JobStatus"]

        print("[textract] Status:", status)

        if status == "SUCCEEDED":
            break

        if status == "FAILED":
            raise Exception("Textract OCR failed")

        time.sleep(3)

    lines = []

    while True:
        for block in result.get("Blocks", []):
            if block.get("BlockType") == "LINE":
                lines.append(block.get("Text", ""))

        next_token = result.get("NextToken")

        if not next_token:
            break

        result = textract.get_document_text_detection(
            JobId=job_id,
            NextToken=next_token,
        )

    text = "\n".join(lines)

    print("[textract] Extracted text length:", len(text.strip()))

    return text