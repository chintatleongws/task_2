"""Bright Data API adapter for the social-media ingestion pipeline."""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class BrightDataRequestError(RuntimeError):
    """Raised when a Bright Data request fails after all attempts."""


class BrightDataAPI:
    """Submit social-media URLs to the Bright Data Datasets API."""

    DATASETS = {
        "instagram_profile": "gd_l1vikfch901nx3by4",
        "instagram_post": "gd_lk5ns7kz21pck8jpis",
        "instagram_comment": "gd_ltppn085pokosxh13",
        "instagram_video": "gd_lyclm20il4r5helnj",
        "tiktok_profile": "gd_l1villgoiiidt09ci",
        "tiktok_video": "gd_lu702nij2f790tmv9h",
        "tiktok_comment": "gd_lkf2st302ap89utw5k",
        "facebook_profile": "gd_mf0urb782734ik94dz",
        "facebook_post": "gd_lyclm1571iy3mv57zw",
        "facebook_comment": "gd_lkay758p1eanlolqw8",
        "reddit_post": "gd_lvz8ah06191smkebj4",
        "reddit_comments": "gd_lvzdpsdlw09j6t702",
        "youtube_video": "gd_lk56epmy2i5g7lzu0k",
        "youtube_channel": "gd_lk538t2k2p1k3oos71",
        "youtube_comments": "gd_lk9q0ew71spt1mxywf",
    }

    DATASET_ROUTES = {
        ("instagram", "profile"): "instagram_profile",
        ("instagram", "post"): "instagram_post",
        ("instagram", "video"): "instagram_video",
        ("instagram", "comment"): "instagram_comment",
        ("facebook", "profile"): "facebook_profile",
        ("facebook", "post"): "facebook_post",
        ("facebook", "video"): "facebook_post",
        ("facebook", "comment"): "facebook_comment",
        ("tiktok", "profile"): "tiktok_profile",
        ("tiktok", "post"): "tiktok_video",
        ("tiktok", "video"): "tiktok_video",
        ("tiktok", "comment"): "tiktok_comment",
        ("youtube", "profile"): "youtube_channel",
        ("youtube", "channel"): "youtube_channel",
        ("youtube", "post"): "youtube_video",
        ("youtube", "video"): "youtube_video",
        ("youtube", "comment"): "youtube_comments",
        ("reddit", "post"): "reddit_post",
        ("reddit", "comment"): "reddit_comments",
    }

    ENTITY_ALIASES = {
        "profile": "profile",
        "profiles": "profile",
        "channel": "channel",
        "channels": "channel",
        "post": "post",
        "posts": "post",
        "video": "video",
        "videos": "video",
        "comment": "comment",
        "comments": "comment",
    }

    def __init__(
        self,
        api_key: str | None = None,
        max_retries: int = 3,
        retry_delay: float = 2.0,
        storage_path: str | Path | None = None,
    ):
        self.base_url = "https://api.brightdata.com/datasets/v3"
        self.api_key = api_key or os.getenv("BRIGHTDATA_API_KEY")
        if not self.api_key:
            raise ValueError("BRIGHTDATA_API_KEY is not set")
        if max_retries < 0:
            raise ValueError("max_retries must be zero or greater")
        if retry_delay < 0:
            raise ValueError("retry_delay must be zero or greater")

        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.storage_path = Path(
            storage_path
            or os.getenv("BRONZE_PATH", "./data/bronze/raw_json")
        )
        self.datasets = dict(self.DATASETS)
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    @staticmethod
    def normalize_entity(entity: str) -> str:
        """Normalize singular and plural entity names."""
        normalized = BrightDataAPI.ENTITY_ALIASES.get(entity.lower().strip())
        if normalized is None:
            raise ValueError(
                f"Unsupported entity '{entity}'. Expected profile, channel, "
                "post, video, or comment."
            )
        return normalized

    @staticmethod
    def infer_platform(url: str) -> str:
        """Infer a supported platform from an exact URL hostname."""
        parsed = urlparse(url.strip())
        hostname = (parsed.hostname or "").lower()
        if parsed.scheme not in {"http", "https"} or not hostname:
            raise ValueError(f"Invalid URL: {url}")

        def matches(domain: str) -> bool:
            return hostname == domain or hostname.endswith(f".{domain}")

        if matches("instagram.com"):
            return "instagram"
        if matches("facebook.com") or matches("fb.com"):
            return "facebook"
        if matches("tiktok.com"):
            return "tiktok"
        if matches("youtube.com") or matches("youtu.be"):
            return "youtube"
        if matches("reddit.com") or matches("redd.it"):
            return "reddit"

        raise ValueError(f"Unable to infer platform from URL: {url}")

    def resolve_dataset_key(self, platform: str, entity: str) -> str:
        """Resolve a platform/entity pair to a configured dataset key."""
        platform = platform.lower().strip()
        entity = self.normalize_entity(entity)
        dataset_key = self.DATASET_ROUTES.get((platform, entity))

        if dataset_key is None:
            raise ValueError(
                f"Entity '{entity}' is not supported for platform '{platform}'."
            )
        if dataset_key not in self.datasets:
            raise ValueError(f"Dataset '{dataset_key}' is not configured.")
        return dataset_key

    def infer_dataset_key(self, url: str) -> str:
        """Infer a dataset from URL structure when no entity is supplied."""
        platform = self.infer_platform(url)
        parsed = urlparse(url)
        path = parsed.path.lower()

        if platform == "instagram":
            if re.search(r"/(p|reel|tv)/", path):
                return "instagram_post"
            return "instagram_profile"
        if platform == "facebook":
            if any(part in path for part in ("/posts/", "/photos/", "/videos/")):
                return "facebook_post"
            return "facebook_profile"
        if platform == "reddit":
            return "reddit_post"
        if platform == "youtube":
            if (
                (parsed.hostname or "").lower().endswith("youtu.be")
                or path == "/watch"
                or path.startswith(("/shorts/", "/embed/", "/live/"))
            ):
                return "youtube_video"
            return "youtube_channel"
        if platform == "tiktok":
            if "/video/" in path:
                return "tiktok_video"
            return "tiktok_profile"

        raise ValueError(f"Unable to infer dataset for URL '{url}'.")

    def build_job_payload(
        self,
        urls: list[str],
        dataset_key: str | None = None,
        job_id: str = "job",
    ) -> dict[str, Any]:
        """Build a queue-friendly Bright Data scrape description."""
        if not urls:
            raise ValueError("At least one URL is required")

        dataset_key = dataset_key or self.infer_dataset_key(urls[0])
        if dataset_key not in self.datasets:
            raise ValueError(f"Unknown dataset key '{dataset_key}'.")

        return {
            "id": job_id,
            "source": "brightdata",
            "data": {
                "dataset": dataset_key,
                "dataset_id": self.datasets[dataset_key],
                "urls": urls,
            },
        }

    def scrape(self, urls: list[str], entity: str) -> dict[str, Any]:
        """Scrape same-entity URLs, grouping them by platform dataset."""
        entity = self.normalize_entity(entity)
        if not urls:
            return {"entity": entity, "results": {}, "rejected": []}

        grouped_urls: dict[str, list[str]] = {}
        rejected: list[dict[str, str]] = []

        for url in urls:
            try:
                platform = self.infer_platform(url)
                dataset_key = self.resolve_dataset_key(platform, entity)
                grouped_urls.setdefault(dataset_key, []).append(url)
            except ValueError as error:
                rejected.append({"url": url, "error": str(error)})

        completed_groups = []
        for dataset_key, dataset_urls in grouped_urls.items():
            try:
                result = self._run(
                    dataset_id=self.datasets[dataset_key],
                    dataset_key=dataset_key,
                    urls=dataset_urls,
                )
                completed_groups.append(
                    (
                        dataset_key,
                        {
                            "status": "success",
                            "urls": dataset_urls,
                            **result,
                        },
                    )
                )
            except (
                BrightDataRequestError,
                OSError,
                RuntimeError,
                TimeoutError,
                ValueError,
            ) as error:
                completed_groups.append(
                    (
                        dataset_key,
                        {
                            "status": "failed",
                            "urls": dataset_urls,
                            "error": str(error),
                        },
                    )
                )

        return {
            "entity": entity,
            "results": dict(completed_groups),
            "rejected": rejected,
        }

    def _run(
        self,
        dataset_id: str,
        dataset_key: str,
        urls: list[str],
    ) -> dict[str, Any]:
        raw_records: list[Any] = []
        for start in range(0, len(urls), 20):
            result = self._submit_scrape(
                dataset_id,
                urls[start:start + 20],
            )
            if isinstance(result, list):
                raw_records.extend(result)
            else:
                raw_records.append(result)

        valid_records, error_records = self._split_results(raw_records)

        if valid_records:
            self._save_result(valid_records, dataset_key)

        return {
            "records": valid_records,
            "errors": error_records,
            "records_count": len(valid_records),
            "errors_count": len(error_records),
        }

    def _submit_scrape(self, dataset_id: str, urls: list[str]) -> Any:
        """Submit a scrape and resolve a snapshot response when required."""
        if dataset_id not in self.datasets.values():
            raise ValueError(
                f"Dataset '{dataset_id}' is not recognized. "
                f"Available datasets: {list(self.datasets.keys())}"
            )
        if not urls:
            raise ValueError("At least one URL is required")

        payload = {"input": [{"url": url} for url in urls]}
        url = (
            f"{self.base_url}/scrape"
            f"?dataset_id={dataset_id}&include_errors=true"
        )
        response_json = self._request_with_retries("post", url, payload)

        if isinstance(response_json, dict) and "snapshot_id" in response_json:
            return self._wait_for_snapshot(response_json["snapshot_id"])
        return response_json

    def _request_with_retries(
        self,
        method: str,
        url: str,
        payload: dict[str, Any] | None = None,
    ) -> Any:
        """Send an HTTP request and retry transport, HTTP, or JSON failures."""
        last_error: Exception | None = None

        for attempt in range(self.max_retries + 1):
            try:
                if method == "post":
                    response = requests.post(
                        url,
                        headers=self.headers,
                        json=payload,
                        timeout=(10, 300),
                    )
                elif method == "get":
                    response = requests.get(
                        url,
                        headers=self.headers,
                        timeout=60,
                    )
                else:
                    raise ValueError(f"Unsupported method: {method}")

                if not response.ok:
                    raise BrightDataRequestError(
                        "Bright Data request failed with status code "
                        f"{response.status_code}"
                    )

                try:
                    return response.json()
                except requests.exceptions.JSONDecodeError:
                    return [
                        json.loads(line)
                        for line in response.text.splitlines()
                        if line.strip()
                    ]

            except (
                requests.RequestException,
                BrightDataRequestError,
                json.JSONDecodeError,
            ) as error:
                last_error = error
                if attempt >= self.max_retries:
                    break

                delay = self.retry_delay * (attempt + 1)
                logger.warning(
                    "Bright Data request attempt %s failed; "
                    "retrying in %.1fs: %s",
                    attempt + 1,
                    delay,
                    error,
                )
                time.sleep(delay)

        raise BrightDataRequestError(
            "Bright Data request failed after "
            f"{self.max_retries + 1} attempts: {last_error}"
        ) from last_error

    def _monitor_snapshot(self, snapshot_id: str) -> dict[str, Any]:
        url = f"{self.base_url}/progress/{snapshot_id}"
        progress = self._request_with_retries("get", url)
        if not isinstance(progress, dict):
            raise BrightDataRequestError(
                f"Unexpected progress response for snapshot {snapshot_id}"
            )
        return progress

    def _download_snapshot(self, snapshot_id: str) -> Any:
        url = f"{self.base_url}/snapshot/{snapshot_id}?format=json"
        return self._request_with_retries("get", url)

    def _wait_for_snapshot(
        self,
        snapshot_id: str,
        poll_interval: int = 5,
        max_wait_seconds: int = 900,
    ) -> Any:
        start_time = time.monotonic()

        while True:
            progress = self._monitor_snapshot(snapshot_id)
            status = str(progress.get("status", "")).lower()
            logger.info("Snapshot %s status: %s", snapshot_id, status or "unknown")

            if status in {"ready", "completed", "done"}:
                return self._download_snapshot(snapshot_id)
            if status in {"failed", "error"}:
                raise RuntimeError(f"Scrape failed: {progress}")
            if time.monotonic() - start_time > max_wait_seconds:
                raise TimeoutError(
                    f"Snapshot {snapshot_id} was not ready after "
                    f"{max_wait_seconds} seconds"
                )

            time.sleep(poll_interval)

    @staticmethod
    def _split_results(
        result: Any,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        records = result if isinstance(result, list) else [result]
        valid_records: list[dict[str, Any]] = []
        error_records: list[dict[str, Any]] = []

        for item in records:
            if not isinstance(item, dict):
                error_records.append(
                    {
                        "error": "Unexpected response type",
                        "raw_result": item,
                    }
                )
            elif item.get("error") or item.get("error_code"):
                error_records.append(item)
            else:
                valid_records.append(item)

        return valid_records, error_records

    def _save_result(
        self,
        result: list[dict[str, Any]],
        dataset_key: str,
    ) -> Path:
        self.storage_path.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        output_path = self.storage_path / f"{dataset_key}_{timestamp}.json"

        with output_path.open("w", encoding="utf-8") as file:
            json.dump(result, file, indent=4)

        logger.info(
            "Saved %s %s records to %s",
            len(result),
            dataset_key,
            output_path,
        )
        return output_path


if __name__ == "__main__":
    urls = [
        "https://www.instagram.com/harrystyles/",
        "https://www.instagram.com/taylorswift/",
    ]
    api = BrightDataAPI()
    result = api.scrape(urls, entity="profile")
    print(json.dumps(result, indent=2))
    
