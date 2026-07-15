import json
import os

import boto3
from botocore.config import Config


def get_boto_clients():
    """Create boto3 clients that target LocalStack when available."""
    endpoint_url = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
    region_name = os.getenv("AWS_DEFAULT_REGION", "eu-west-1")
    access_key_id = os.getenv("AWS_ACCESS_KEY_ID", "test")
    secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY", "test")

    session = boto3.session.Session()
    sqs_client = session.client(
        "sqs",
        region_name=region_name,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        endpoint_url=endpoint_url,
        config=Config(signature_version="s3v4"),
    )
    s3_client = session.client(
        "s3",
        region_name=region_name,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        endpoint_url=endpoint_url,
        config=Config(signature_version="s3v4"),
    )
    return sqs_client, s3_client


def get_bucket_name():
    """Return the S3 bucket name used by the local lambda mimic."""
    return os.getenv("S3_BUCKET", "test-bucket")


def process_message_body(body, s3_client=None, bucket_name=None):
    """Store a processed payload in S3/LocalStack-compatible storage."""
    if s3_client is None or bucket_name is None:
        _, s3_client = get_boto_clients()
        bucket_name = get_bucket_name()

    try:
        s3_client.create_bucket(Bucket=bucket_name)
    except Exception:
        pass

    payload = json.dumps({"status": "processed", **body}).encode("utf-8")
    key = f"processed/{body.get('id', 'job')}.json"
    s3_client.put_object(Bucket=bucket_name, Key=key, Body=payload)
    return {"bucket": bucket_name, "key": key}
