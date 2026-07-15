import json
import os
import time
from typing import Any

from utils import get_boto_clients, get_bucket_name, process_message_body

# env settings
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "test")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "eu-west-1")
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL", "http://localhost:4566/000000000000/test-queue")
SQS_QUEUE_NAME = os.getenv("SQS_QUEUE_NAME", "test-queue")
S3_BUCKET = os.getenv("S3_BUCKET", "test-bucket")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))


def ensure_resources(client, s3_client, bucket_name: str) -> str:
    try:
        s3_client.create_bucket(Bucket=bucket_name)
    except Exception:
        pass

    try:
        dlq_response = client.create_queue(QueueName="test-queue-dlq")
    except Exception:
        dlq_response = client.get_queue_url(QueueName="test-queue-dlq")

    dlq_url = dlq_response["QueueUrl"]

    try:
        client.create_queue(
            QueueName=SQS_QUEUE_NAME,
            Attributes={
                "RedrivePolicy": json.dumps(
                    {
                        "deadLetterTargetArn": f"arn:aws:sqs:{AWS_DEFAULT_REGION}:000000000000:test-queue-dlq",
                        "maxReceiveCount": str(MAX_RETRIES + 1),
                    }
                )
            },
        )
    except Exception:
        pass

    return dlq_url


def send_to_dlq(client, message: dict[str, Any], reason: str, dlq_url: str) -> None:
    body = {
        "original_message": message["Body"],
        "reason": reason,
        "attempts": message.get("Attributes", {}).get("ApproximateReceiveCount", "unknown"),
    }
    client.send_message(QueueUrl=dlq_url, MessageBody=json.dumps(body))
    print(f"Sent to DLQ: {reason}")


def process_queue_messages() -> None:
    sqs_client, s3 = get_boto_clients()
    dlq_url = ensure_resources(sqs_client, s3, S3_BUCKET)

    while True:
        response = sqs_client.receive_message(
            QueueUrl=SQS_QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=5,
            AttributeNames=["All"],
        )
        messages = response.get("Messages", [])
        if not messages:
            continue

        message = messages[0]
        try:
            body = json.loads(message["Body"])
            process_message_body(body, s3_client=s3, bucket_name=S3_BUCKET)
            sqs_client.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=message["ReceiptHandle"])
            print(f"Processed {body}")
        except Exception as exc:
            attempts = int(message.get("Attributes", {}).get("ApproximateReceiveCount", "0"))
            print(f"Processing failed (attempt {attempts}/{MAX_RETRIES + 1}): {exc}")

            if attempts >= MAX_RETRIES + 1:
                send_to_dlq(sqs_client, message, str(exc), dlq_url)
                sqs_client.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=message["ReceiptHandle"])
            else:
                time.sleep(1)


if __name__ == "__main__":
    process_queue_messages()

