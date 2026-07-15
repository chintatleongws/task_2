import json
import logging

from utils import get_boto_clients, get_bucket_name

logging.basicConfig(level=logging.INFO)


def lambda_fake(event, context):
    """lambda mimic for processing SQS messages and storing them in S3"""
    logging.info(f"Received event: {json.dumps(event)}")

    _, s3_client = get_boto_clients()
    bucket_name = get_bucket_name()

    for record in event.get("Records", []):
        body = json.loads(record["body"])
        s3_client.put_object(
            Bucket=bucket_name,
            Key=f"processed/{body.get('id', 'job')}.json",
            Body=json.dumps(body),
        )

    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Processed messages successfully"}),
    }