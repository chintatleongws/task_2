import json
import os
from airflow.sdk import dag, task
import pendulum
import boto3
from botocore.config import Config

from scraper_class import BrightDataAPI


@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["BrightDataPipeline"],
)
def BrightDataPipeline(urls: list[str]):
    @task()
    def build_payload(urls: list[str]):
        """Build the Bright Data payload using the adapter's dataset mapping."""
        api = BrightDataAPI(api_key=os.getenv("BRIGHTDATA_API_KEY"))
        return api.build_job_payload(urls=urls, dataset_key="instagram_post", job_id="job_test")

    @task()
    def publish_to_sqs(**context):
        """Send a Bright Data job request to the local SQS queue."""
        payload = build_payload(urls)
        endpoint_url = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
        region_name = os.getenv("AWS_DEFAULT_REGION", "eu-west-1")
        access_key_id = os.getenv("AWS_ACCESS_KEY_ID", "test")
        secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
        queue_url = os.getenv("SQS_QUEUE_URL", "http://localhost:4566/000000000000/test-queue")

        session = boto3.session.Session()
        client = session.client(
            "sqs",
            region_name=region_name,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            endpoint_url=endpoint_url,
            config=Config(signature_version="s3v4"),
        )

        client.send_message(QueueUrl=queue_url, MessageBody=json.dumps(payload))
        return payload



