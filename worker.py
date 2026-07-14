import os
import json
import boto3
from botocore.config import Config

# env settings
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "test")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "eu-west-1")
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL", "http://localstack:4566/000000000000/test-queue")
S3_BUCKET = os.getenv("S3_BUCKET", "test-bucket")

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

try:
    s3.create_bucket(Bucket=S3_BUCKET)
except Exception:
    pass

try:
    client.create_queue(QueueName="test-queue")
except Exception:
    pass

while True:
    response = client.receive_message(QueueUrl=SQS_QUEUE_URL, MaxNumberOfMessages=1, WaitTimeSeconds=5)
    messages = response.get("Messages", [])
    if not messages:
        continue

    message = messages[0]
    body = json.loads(message["Body"])

    payload = json.dumps({"status": "processed", **body}).encode("utf-8")
    s3.put_object(Bucket=S3_BUCKET, Key=f"processed/{body.get('id', 'job')}.json", Body=payload)

    client.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=message["ReceiptHandle"])
    print(f"Processed {body}")
