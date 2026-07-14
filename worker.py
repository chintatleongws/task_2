import os
import json
import time
import boto3
from botocore.config import Config

# env settings
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "test")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "eu-west-1")
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL", "http://localstack:4566/000000000000/test-queue")
SQS_QUEUE_NAME = os.getenv("SQS_QUEUE_NAME", "test-queue")
S3_BUCKET = os.getenv("S3_BUCKET", "test-bucket")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))

session = boto3.session.Session()
client = session.client(
    "sqs",
    region_name=AWS_DEFAULT_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    endpoint_url=AWS_ENDPOINT_URL,
    config=Config(signature_version="s3v4"),
)

s3 = session.client(
    "s3",
    region_name=AWS_DEFAULT_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    endpoint_url=AWS_ENDPOINT_URL,
    config=Config(signature_version="s3v4"),
)


def ensure_resources():
    try:
        s3.create_bucket(Bucket=S3_BUCKET)
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


def send_to_dlq(message, reason):
    body = {
        "original_message": message["Body"],
        "reason": reason,
        "attempts": message.get("Attributes", {}).get("ApproximateReceiveCount", "unknown"),
    }
    client.send_message(QueueUrl=DLQ_URL, MessageBody=json.dumps(body))
    print(f"Sent to DLQ: {reason}")


DLQ_URL = ensure_resources()


while True:
    response = client.receive_message(
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
        payload = json.dumps({"status": "processed", **body}).encode("utf-8")
        s3.put_object(Bucket=S3_BUCKET, Key=f"processed/{body.get('id', 'job')}.json", Body=payload)
        client.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=message["ReceiptHandle"])
        print(f"Processed {body}")
    except Exception as exc:
        attempts = int(message.get("Attributes", {}).get("ApproximateReceiveCount", "0"))
        print(f"Processing failed (attempt {attempts}/{MAX_RETRIES + 1}): {exc}")

        if attempts >= MAX_RETRIES + 1:
            send_to_dlq(message, str(exc))
            client.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=message["ReceiptHandle"])
        else:
            time.sleep(1)

