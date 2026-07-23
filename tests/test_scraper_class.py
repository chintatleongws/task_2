import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import requests

from scraper_class import BrightDataAPI, BrightDataRequestError


class BrightDataAPITests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.api = BrightDataAPI(
            api_key="test-key",
            max_retries=1,
            retry_delay=0,
            storage_path=self.temp_dir.name,
        )

    def test_infers_platform_and_specific_profile_datasets(self):
        self.assertEqual(
            self.api.infer_dataset_key("https://www.youtube.com/@MrBeast"),
            "youtube_channel",
        )
        self.assertEqual(
            self.api.infer_dataset_key("https://www.tiktok.com/@creator"),
            "tiktok_profile",
        )
        self.assertEqual(
            self.api.infer_dataset_key(
                "https://www.tiktok.com/@creator/video/123"
            ),
            "tiktok_video",
        )

    def test_rejects_spoofed_and_invalid_urls(self):
        for url in [
            "https://instagram.com.example.org/profile",
            "not-a-url",
        ]:
            with self.subTest(url=url), self.assertRaises(ValueError):
                self.api.infer_platform(url)

    def test_build_job_payload_rejects_empty_urls(self):
        with self.assertRaisesRegex(ValueError, "At least one URL"):
            self.api.build_job_payload([])

    @patch("scraper_class.requests.post")
    def test_http_status_failures_are_retried(self, mock_post):
        failed = Mock(ok=False, status_code=500)
        succeeded = Mock(ok=True)
        succeeded.json.return_value = [{"id": "4"}]
        mock_post.side_effect = [failed, succeeded]

        result = self.api._request_with_retries(
            "post",
            "https://api.example.test/scrape",
            {"input": []},
        )

        self.assertEqual(result, [{"id": "4"}])
        self.assertEqual(mock_post.call_count, 2)

    @patch("scraper_class.requests.post")
    def test_ndjson_fallback_is_reachable(self, mock_post):
        response = Mock(ok=True)
        response.json.side_effect = requests.exceptions.JSONDecodeError(
            "not a JSON document",
            "",
            0,
        )
        response.text = '{"id": "1"}\n{"id": "2"}\n'
        mock_post.return_value = response

        result = self.api._request_with_retries(
            "post",
            "https://api.example.test/scrape",
            {"input": []},
        )

        self.assertEqual(result, [{"id": "1"}, {"id": "2"}])

    def test_scrape_returns_flat_group_result_and_saves_valid_records(self):
        self.api._submit_scrape = Mock(
            return_value=[
                {
                    "id": "4",
                    "url": "https://www.facebook.com/zuck/",
                }
            ]
        )

        result = self.api.scrape(
            ["https://www.facebook.com/zuck/"],
            entity="profile",
        )

        group = result["results"]["facebook_profile"]
        self.assertEqual(group["status"], "success")
        self.assertEqual(group["records_count"], 1)
        self.assertEqual(group["errors_count"], 0)
        self.assertEqual(group["records"][0]["id"], "4")

        files = list(Path(self.temp_dir.name).glob("facebook_profile_*.json"))
        self.assertEqual(len(files), 1)
        with files[0].open(encoding="utf-8") as file:
            self.assertEqual(json.load(file)[0]["id"], "4")

    def test_large_dataset_groups_are_submitted_in_batches_of_twenty(self):
        self.api._submit_scrape = Mock(
            side_effect=[
                [{"id": str(index)} for index in range(20)],
                [{"id": "20"}],
            ]
        )
        self.api._save_result = Mock()
        urls = [
            f"https://www.instagram.com/profile-{index}"
            for index in range(21)
        ]

        result = self.api.scrape(urls, entity="profile")

        self.assertEqual(self.api._submit_scrape.call_count, 2)
        first_urls = self.api._submit_scrape.call_args_list[0].args[1]
        second_urls = self.api._submit_scrape.call_args_list[1].args[1]
        self.assertEqual(len(first_urls), 20)
        self.assertEqual(len(second_urls), 1)
        self.assertEqual(
            result["results"]["instagram_profile"]["records_count"],
            21,
        )

    def test_exhausted_http_failures_raise_domain_error(self):
        response = Mock(ok=False, status_code=503)
        with patch("scraper_class.requests.get", return_value=response):
            with self.assertRaises(BrightDataRequestError):
                self.api._request_with_retries(
                    "get",
                    "https://api.example.test/progress/snapshot",
                )

if __name__ == "__main__":
    unittest.main()
