"""
Class for connecting to the BrightData API 
"""

import os
import requests
import time
from dotenv import load_dotenv
from types import List, Dict, Any 

load_dotenv()

class BrightDataAPI:
    def __init__(self, api_key: str | None = None):
        self.base_url = "https://api.brightdata.com/datasets/v3"
        self.api_key = api_key or os.getenv("BRIGHTDATA_API_KEY")

        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        # Bright Data dataset IDs for individual calls to different endpoints
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
            "reddit_post": "gd_lvz8ah06191smkebj4",
            "reddit_comments": "gd_lvzdpsdlw09j6t702",
            # couldnt find a dataset for reddit profiles.
            "youtube_video": "gd_lk56epmy2i5g7lzu0k",
            "youtube_channel": "gd_lk538t2k2p1k3oos71",
            "youtube_comments": "gd_lk9q0ew71spt1mxywf",
        }

    def _get_sync(self, dataset_id: str, urls: List[str], output_format: str = "json") -> Dict[str, Any]:
        """
        Make a synchronous request to the BrightData API for the specified dataset and URLs. 
        Can be up to 20 URLs per request, in real time. 
        """
        if dataset_id not in self.datasets:
            raise ValueError(f"Dataset '{dataset_id}' is not recognized. Available datasets: {list(self.datasets.keys())}")

        if len(urls) > 20:
            raise ValueError("Sync requests support up to 20 URLs. Use scrape_async for larger jobs.")

        payload = [{"url": url} for url in urls]
        url = f"{self.base_url}/scrape?dataset_id={dataset_id}&format={output_format}"

        response = requests.post(url, headers=self.headers, json=payload)
        response.raise_for_status()
        return response.json()
    
    def _get_async(self, dataset_id: str, urls: List[str]) -> Dict[str, Any]:
        """
        Make an asynchronous request to the BrightData API for the specified dataset and URLs. 
        Designed for larger requests and production use. Returns a job ID that can be used to check the status of the request and retrieve results later.
        """
        if dataset_id not in self.datasets:
            raise ValueError(f"Dataset '{dataset_id}' is not recognized. Available datasets: {list(self.datasets.keys())}")

        payload = [{"url": url} for url in urls]
        url = f"{self.base_url}/trigger?dataset_id={dataset_id}"

        response = requests.post(url, headers=self.headers, json=payload)
        response.raise_for_status()
        return response.json()["snapshot_id"]
    
    def get_snapshot(self, snapshot_id: str) -> Dict[str, Any]:
        url = f"{self.base_url}/snapshot/{snapshot_id}"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()

    def wait_for_snapshot(self, snapshot_id: str, poll_interval: int = 5) -> Any:
        while True:
            snapshot = self.get_snapshot(snapshot_id)
            status = snapshot.get("status")

            if status == "ready":
                return snapshot.get("data", snapshot)
            if status == "failed":
                raise Exception(f"Scrape failed: {snapshot}")

            time.sleep(poll_interval)

    # =========================================================================
    # Maybe this is bulky but it makes the interface cleaner for the user.
    # To do:
    # - change copy paste job for scapes to more specific functions for each type.
    # - maybe some sort of api key retention
    # =========================================================================

    def scrape_instagram_profiles(self, urls: List[str], async_mode: bool = False):
        dataset_id = self.datasets["instagram_profile"]
        return self._run(dataset_id, urls, async_mode)

    def scrape_instagram_posts(self, urls: List[str], async_mode: bool = False):
        dataset_id = self.datasets["instagram_post"]
        return self._run(dataset_id, urls, async_mode)
    
    def scrape_instagram_comments(self, urls: List[str], async_mode: bool = False):
        dataset_id = self.datasets["instagram_comment"]
        return self._run(dataset_id, urls, async_mode)
    
    def scrape_instagram_reels(self, urls: List[str], async_mode: bool = False):
        dataset_id = self.datasets["instagram_reel"]
        return self._run(dataset_id, urls, async_mode)

    def scrape_tiktok_profiles(self, urls: List[str], async_mode: bool = False):
        dataset_id = self.datasets["tiktok_profile"]
        return self._run(dataset_id, urls, async_mode)

    def scrape_tiktok_posts(self, urls: List[str], async_mode: bool = False):
        dataset_id = self.datasets["tiktok_post"]
        return self._run(dataset_id, urls, async_mode)
    
    def scrape_facebook_profiles(self, urls: List[str], async_mode: bool = False):
        dataset_id = self.datasets["facebook_profile"]
        return self._run(dataset_id, urls, async_mode)
    
    def scrape_facebook_posts(self, urls: List[str], async_mode: bool = False):
        dataset_id = self.datasets["facebook_post"]
        return self._run(dataset_id, urls, async_mode)
    
    def scrape_reddit_posts(self, urls: List[str], async_mode: bool = False):
        dataset_id = self.datasets["reddit_post"]
        return self._run(dataset_id, urls, async_mode)
    
    def scrape_reddit_comments(self, urls: List[str], async_mode: bool = False):
        dataset_id = self.datasets["reddit_comments"]
        return self._run(dataset_id, urls, async_mode)
    
    def scrape_youtube_videos(self, urls: List[str], async_mode: bool = False):
        dataset_id = self.datasets["youtube_video"]
        return self._run(dataset_id, urls, async_mode)
    
    def scrape_youtube_channels(self, urls: List[str], async_mode: bool = False):
        dataset_id = self.datasets["youtube_channel"]
        return self._run(dataset_id, urls, async_mode)
    
    def scrape_youtube_comments(self, urls: List[str], async_mode: bool = False):
        dataset_id = self.datasets["youtube_comments"]
        return self._run(dataset_id, urls, async_mode)

    def _run(self, dataset_id: str, urls: List[str], async_mode: bool):
        if async_mode or len(urls) > 20:
            snapshot_id = self._get_async(dataset_id, urls)
            return self.wait_for_snapshot(snapshot_id)
        return self._get_sync(dataset_id, urls)