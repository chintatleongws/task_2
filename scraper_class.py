"""BrightData API adapter for use in a wider orchestration pipeline."""

from __future__ import annotations

import asyncio
import json
import os
import re
import time
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List
from unittest import result
from urllib import response

import requests
from dotenv import load_dotenv


class BrightDataRequestError(RuntimeError):
    """Raised when a BrightData request fails after retry attempts."""

load_dotenv()

class STORAGE_MODE(str, Enum):
    LOCAL = "local"
    S3 = "s3"
    GCS = "gcs"
    AZURE = "azure"
    
storage_mode = STORAGE_MODE(os.getenv("STORAGE_MODE", "local").lower())

if storage_mode == STORAGE_MODE.LOCAL:
    storage_path = "./data/bronze/raw_json"
elif storage_mode == STORAGE_MODE.S3:
    storage_path = "s3://your-bucket-name/data/bronze/raw_json"
elif storage_mode == STORAGE_MODE.GCS:
    storage_path = "gs://your-bucket-name/data/bronze/raw_json"
elif storage_mode == STORAGE_MODE.AZURE:
    storage_path = "https://your-account-name.blob.core.windows.net/your-container-name/data/bronze/raw_json"
    
    


class BrightDataAPI:
    """Thin wrapper around the BrightData datasets API."""

    def __init__(self, api_key: str | None = None, max_retries: int = 3, retry_delay: float = 2.0):
        self.base_url = "https://api.brightdata.com/datasets/v3"
        self.api_key = api_key or os.getenv("BRIGHTDATA_API_KEY")
        if not self.api_key:
            raise ValueError("BRIGHTDATA_API_KEY is not set")
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        self.datasets = {
            "instagram_profile": "gd_l1vikfch901nx3by4",
            "instagram_post": "gd_lk5ns7kz21pck8jpis",
            "instagram_comment": "gd_ltppn085pokosxh13",
            "instagram_reel": "gd_lyclm20il4r5helnj",
            "tiktok_profile": "gd_l1villgoiiidt09ci",
            "tiktok_post": "gd_lu702nij2f790tmv9h",
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

    def infer_dataset_key(self, url: str) -> str:
        """Infer the BrightData dataset key from a single URL."""
        lowered = url.lower()
        if "instagram.com" in lowered:
            if re.search(r"/p/|/reel/|/tv/", lowered):
                return "instagram_post"
            return "instagram_profile"
        if "facebook.com" in lowered:
            if "/posts/" in lowered or "/photos/" in lowered or "/videos/" in lowered:
                return "facebook_post"
            return "facebook_profile"
        if "reddit.com" in lowered:
            return "reddit_post"
        if "youtube.com" in lowered or "youtu.be" in lowered:
            return "youtube_video"
        if "tiktok.com" in lowered:
            return "tiktok_post"
        raise ValueError(f"Unable to infer dataset for URL '{url}'.")

    def build_job_payload(self, urls: List[str], dataset_key: str | None = None, job_id: str = "job") -> Dict[str, Any]:
        """Build a queue-friendly payload from the adapter's dataset mapping.

        If no dataset key is provided, the adapter will infer one from the URL patterns.
        """
        if dataset_key is None:
            dataset_key = self.infer_dataset_key(urls[0])
        elif dataset_key not in self.datasets:
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

    def _get_sync(self, dataset_id: str, urls: List[str], output_format: str = "json") -> Dict[str, Any]:
        """Make a synchronous request to BrightData for up to 20 URLs."""
        if dataset_id not in self.datasets.values():
            raise ValueError(
                f"Dataset '{dataset_id}' is not recognized. Available datasets: {list(self.datasets.keys())}"
            )

        if len(urls) > 20:
            raise ValueError("Sync requests support up to 20 URLs. Use async mode for larger jobs.")

        payload = {
            "input": [{"url": url} for url in urls]
        }
        url = f"{self.base_url}/scrape?dataset_id={dataset_id}&include_errors=true"

        response_json = self._request_with_retries("post", url, payload)

        if isinstance(response_json, dict) and "snapshot_id" in response_json:
            snapshot_id = response_json["snapshot_id"]
            return self.wait_for_snapshot(snapshot_id)
        
        return response_json

    def _get_async(self, dataset_id: str, urls: List[str], output_format: str = "json") -> str:
        """Trigger an async BrightData request and return the snapshot id."""
        if dataset_id not in self.datasets.values():
            raise ValueError(
                f"Dataset '{dataset_id}' is not recognized. Available datasets: {list(self.datasets.values())}"
            )

        payload = {
            "input": [{"url": url} for url in urls]
        }
        url = f"{self.base_url}/scrape?dataset_id={dataset_id}&include_errors=true"

        response_json = self._request_with_retries("post", url, payload)
        return response_json["snapshot_id"]

    def _request_with_retries(self, method: str, url: str, payload: Dict[str, Any]| None = None) -> Dict[str, Any]:
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            try:
                if method == "post":
                    response = requests.post(
                        url,
                        headers=self.headers,
                        json=payload,
                        timeout=(10, 300)  # 10 sec connect, 300 sec read
                    )
                elif method == "get":
                    response = requests.get(url, headers=self.headers, timeout=60)
                else:
                    raise ValueError(f"Unsupported method: {method}")

                if not response.ok:
                    raise BrightDataRequestError(f"BrightData request failed with status code {response.status_code}: {response.text}")

                response.raise_for_status()
                try:
                    print(f"Response JSON: {response.json()}")  # Debugging line
                    return response.json()
                except requests.exceptions.JSONDecodeError:
                    print(f"Response JSON: {response.json()}")  # Debugging line
                    return [json.loads(line) for line in response.text.splitlines() if line.strip()]
            except requests.RequestException as exc:
                last_error = exc
                if attempt >= self.max_retries:
                    break
                time.sleep(self.retry_delay * (attempt + 1))

        raise BrightDataRequestError(f"BrightData request failed after {self.max_retries} retries: {last_error}") from last_error
    
    def monitor_snapshot(self, snapshot_id: str) -> Dict[str, Any]:
        """Check BrightData snapshot progress."""
        url = f"{self.base_url}/progress/{snapshot_id}"
        return self._request_with_retries("get", url)

    def download_snapshot(self, snapshot_id: str) -> Any:
        """Download BrightData snapshot once ready."""
        url = f"{self.base_url}/snapshot/{snapshot_id}?format=json"
        return self._request_with_retries("get", url)


    def wait_for_snapshot(self, snapshot_id: str, poll_interval: int = 5, max_wait_seconds: int = 900) -> Any:
        start_time = time.time()
        
        while True:
            progress = self.monitor_snapshot(snapshot_id)
            print(f"Snapshot progress: {progress}")

            status = str(progress.get("status", "")).lower()

            if status in ["ready", "completed", "done"]:
                return self.download_snapshot(snapshot_id)

            if status in ["failed", "error"]:
                raise RuntimeError(f"Scrape failed: {progress}")

            if time.time() - start_time > max_wait_seconds:
                raise TimeoutError(f"Snapshot {snapshot_id} was not ready after {max_wait_seconds} seconds")

            time.sleep(poll_interval)
    
    def has_errors(self, result: Any) -> bool:
        if not isinstance(result, list):
            return False

        for item in result:
            if (
                isinstance(item, dict)
                and item.get("error_code")
            ):
                return True

        return False
            
    def save_result(self, result: Any, dataset_key: str):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        if "comment" in dataset_key:
            folder = os.path.join(storage_path, "comments")
        elif "post" in dataset_key:
            folder = os.path.join(storage_path, "posts")
        else:
            folder = storage_path

        os.makedirs(folder, exist_ok=True)

        filename = f"{dataset_key}_{timestamp}.json"

        with open(
            os.path.join(folder, filename),
            "w",
            encoding="utf-8",
        ) as f:
            json.dump(result, f, indent=4)

    async def scrape_instagram_profiles(self, urls: List[str], async_mode: bool = False):
        return await self._run(self.datasets["instagram_profile"], "instagram_profile", urls, async_mode)

    async def scrape_instagram_posts(self, urls: List[str], async_mode: bool = False):
        return await self._run(self.datasets["instagram_post"], "instagram_post", urls, async_mode)

    async def scrape_instagram_comments(self, urls: List[str], async_mode: bool = False):
        return await self._run(self.datasets["instagram_comment"], "instagram_comment", urls, async_mode)

    async def scrape_instagram_reels(self, urls: List[str], async_mode: bool = False):
        return await self._run(self.datasets["instagram_reel"], "instagram_reel", urls, async_mode)

    async def scrape_tiktok_profiles(self, urls: List[str], async_mode: bool = False):
        return await self._run(self.datasets["tiktok_profile"], "tiktok_profile", urls, async_mode)

    async def scrape_tiktok_posts(self, urls: List[str], async_mode: bool = False):
        return await self._run(self.datasets["tiktok_post"], "tiktok_post", urls, async_mode)

    async def scrape_tiktok_comments(self, urls: List[str], async_mode: bool = False):
        return await self._run(self.datasets["tiktok_comment"], "tiktok_comment", urls, async_mode)

    async def scrape_facebook_profiles(self, urls: List[str], async_mode: bool = False):
        return await self._run(self.datasets["facebook_profile"], "facebook_profile", urls, async_mode)

    async def scrape_facebook_posts(self, urls: List[str], async_mode: bool = False):
        return await self._run(self.datasets["facebook_post"], "facebook_post", urls, async_mode)

    async def scrape_facebook_comments(self, urls: List[str], async_mode: bool = False):
        return await self._run(self.datasets["facebook_comment"], "facebook_comment", urls, async_mode)

    async def scrape_reddit_posts(self, urls: List[str], async_mode: bool = False):
        return await self._run(self.datasets["reddit_post"], "reddit_post", urls, async_mode)

    async def scrape_reddit_comments(self, urls: List[str], async_mode: bool = False):
        return await self._run(self.datasets["reddit_comments"], "reddit_comments", urls, async_mode)

    async def scrape_youtube_videos(self, urls: List[str], async_mode: bool = False):
        return await self._run(self.datasets["youtube_video"], "youtube_video", urls, async_mode)

    async def scrape_youtube_channels(self, urls: List[str], async_mode: bool = False):
        return await self._run(self.datasets["youtube_channel"], "youtube_channel", urls, async_mode)

    async def scrape_youtube_comments(self, urls: List[str], async_mode: bool = False):
        return await self._run(self.datasets["youtube_comments"], "youtube_comments", urls, async_mode)

    async def scrape_profiles(self, profiles: List[Dict[str, Any]], dataset_key: str, async_mode: bool = False) -> List[Dict[str, Any]]:
        """Scrape a batch of profile-like objects and return normalized records."""
        if not profiles:
            return []

        if dataset_key not in self.datasets:
            raise ValueError(f"Unknown dataset key '{dataset_key}'.")

        urls = [profile["url"] for profile in profiles]
        dataset_id = self.datasets[dataset_key]
        
        raw_results = await self._run(dataset_id, dataset_key, urls, async_mode)

        if not isinstance(raw_results, list):
            raw_results = [raw_results]

        normalized_results: List[Dict[str, Any]] = []
        for profile, result in zip(profiles, raw_results):
            normalized_results.append(
                {
                    "profile_id": profile.get("profile_id"),
                    "platform": profile.get("platform"),
                    "url": profile.get("url"),
                    "dataset_key": dataset_key,
                    "raw_result": result,
                }
            )
        return normalized_results

    async def _run(
        self,
        dataset_id: str,
        dataset_key: str,
        urls: List[str],
        async_mode: bool,
    ):
        if async_mode or len(urls) > 20:
            snapshot_id = self._get_async(dataset_id, urls)
            result = self.wait_for_snapshot(snapshot_id)
        else:
            result = self._get_sync(dataset_id, urls)

        if self.has_errors(result):
            raise BrightDataRequestError(
                f"Bright Data returned an error: {result}"
            )

        self.save_result(result, dataset_key)

        return result


async def main():
    api = BrightDataAPI()
    urls = ["https://www.instagram.com/reel/C85BZjeSHuO"]
    result = await api.scrape_instagram_reels(urls, async_mode=False)
    print(result)
    
if __name__ == "__main__":
    asyncio.run(main())
