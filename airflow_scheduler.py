import json
import os
from typing import Any

import pendulum
from airflow.decorators import dag, task

from scraper_class import BrightDataAPI
from utils import get_boto_clients

DEFAULT_URLS = [
    "https://www.instagram.com/reel/C85BZjeSHuO",
]
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL", "http://localhost:4566/000000000000/test-queue")
SQS_QUEUE_NAME = os.getenv("SQS_QUEUE_NAME", "test-queue")


def _get_urls() -> list[str]:
    """Return URLs from the environment or fall back to a default sample URL."""
    raw_urls = os.getenv("SCRAPE_URLS", "")
    if raw_urls:
        return [url.strip() for url in raw_urls.split(",") if url.strip()]
    return DEFAULT_URLS


def _ensure_queue(sqs_client) -> None:
    """Create the mock queue if it does not already exist."""
    try:
        sqs_client.create_queue(QueueName=SQS_QUEUE_NAME)
    except Exception:
        pass


@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["BrightData", "mock"],
)
def BrightDataPipeline():
    @task()
    def build_payload() -> list[dict[str, Any]]:
        """Create one or more BrightData scrape payloads from configured URLs.

        Behavior:
        - If `BRIGHTDATA_DATASET_KEY` or `BRIGHTDATA_DATASET_KEYS` is set, build payload(s)
          for the provided dataset(s) using all configured URLs.
        - Otherwise, infer dataset keys per-URL and group URLs by inferred dataset,
          producing one payload per dataset.
        """
        api = BrightDataAPI(api_key=os.getenv("BRIGHTDATA_API_KEY"))
        urls = _get_urls()
        ts = pendulum.now("UTC").strftime("%Y%m%d%H%M%S")

        override = os.getenv("BRIGHTDATA_DATASET_KEYS") or os.getenv("BRIGHTDATA_DATASET_KEY")
        payloads: list[dict[str, Any]] = []

        if override:
            keys = [k.strip() for k in override.split(",") if k.strip()]
            for i, key in enumerate(keys):
                job_id = f"job_{key}_{ts}_{i}"
                payloads.append(api.build_job_payload(urls=urls, dataset_key=key, job_id=job_id))
            return payloads

        # No override: infer dataset by URL and group
        groups: dict[str, list[str]] = {}
        for url in urls:
            try:
                key = api.infer_dataset_key(url)
            except ValueError:
                key = os.getenv("BRIGHTDATA_DATASET_KEY", "instagram_post")
            groups.setdefault(key, []).append(url)

        for key, group_urls in groups.items():
            job_id = f"job_{key}_{ts}"
            payloads.append(api.build_job_payload(urls=group_urls, dataset_key=key, job_id=job_id))

        return payloads

    @task()
    def publish_to_sqs(payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Publish each payload to the local SQS-compatible queue and return send results."""
        sqs_client, _ = get_boto_clients()
        _ensure_queue(sqs_client)

        results: list[dict[str, Any]] = []
        for payload in payloads:
            response = sqs_client.send_message(QueueUrl=SQS_QUEUE_URL, MessageBody=json.dumps(payload))
            results.append({"queue_url": SQS_QUEUE_URL, "message_id": response.get("MessageId"), "payload_id": payload.get("id")})

        return results

    payloads = build_payload()
    return publish_to_sqs(payloads)