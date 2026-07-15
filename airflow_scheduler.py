import json
import os

from airflow.sdk import dag, task
import pendulum

from scraper_class import BrightDataAPI
from utils import get_boto_clients


@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["BrightData"],
)
def BrightDataPipeline(urls: list[str]):
    @task()
    def build_payload(urls: list[str]):
        """Build the Bright Data payload using the adapter's dataset mapping."""
        api = BrightDataAPI(api_key=os.getenv("BRIGHTDATA_API_KEY"))
        return api.build_job_payload(urls=urls, dataset_key="instagram_post", job_id="job_test")

    @task()
    def publish_to_sqs(urls: list[str]):
        """Build a payload and return it for downstream local use.

        This is intentionally dry-friendly: it does not assume real AWS services are available.
        """
        payload = build_payload(urls)
        return payload

    return publish_to_sqs(urls=urls)


if __name__ == "__main__":
    urls = [
        "https://www.instagram.com/p/Cn1J3k5L0aX/",
        "https://www.instagram.com/p/Cn1J3k5L0aY/", # this is AI chosen, probs doesnt work. 
    ]
    print(BrightDataPipeline(urls=urls))
