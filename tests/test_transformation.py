import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from transformation import Transformation


class TransformationTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        root = Path(self.temp_dir.name)
        self.bronze = root / "bronze"
        self.silver = root / "silver"
        self.bronze.mkdir()

    def write_json(self, filename, records):
        path = self.bronze / filename
        with path.open("w", encoding="utf-8") as file:
            json.dump(records, file)
        return path

    def transformer(self):
        return Transformation(self.bronze, self.silver)

    def test_canonical_scraper_filenames_are_recognized(self):
        self.write_json(
            "instagram_post_20260101_000000.json",
            [{"post_id": "ig-1", "url": "https://instagram.com/p/ig-1"}],
        )
        self.write_json(
            "facebook_post_20260101_000000.json",
            [{"post_id": "fb-1", "url": "https://facebook.com/posts/fb-1"}],
        )
        self.write_json(
            "reddit_post_20260101_000000.json",
            [{"post_id": "re-1", "url": "https://reddit.com/r/test/re-1"}],
        )
        self.write_json(
            "youtube_comments_20260101_000000.json",
            [
                {
                    "comment_id": "yt-comment-1",
                    "comment_text": "Great video!",
                    "likes": 2,
                    "replies": 0,
                    "username": "@username",
                    "user_channel": "https://www.youtube.com/@username",
                    "date": "2026-01-01T00:00:00Z",
                    "url": "https://www.youtube.com/watch?v=abc123",
                    "video_id": "abc123",
                    "user_id": "channel-1",
                }
            ],
        )

        outputs = self.transformer().run()

        self.assertEqual(len(outputs["posts"]), 3)
        self.assertEqual(len(outputs["comments"]), 1)
        self.assertEqual(
            outputs["comments"].iloc[0]["platform"],
            "youtube",
        )
        self.assertEqual(outputs["errors"], [])

    def test_empty_input_writes_header_only_tables(self):
        outputs = self.transformer().run()

        for entity in ("posts", "comments", "videos", "profiles"):
            with self.subTest(entity=entity):
                self.assertTrue(outputs[entity].empty)
                self.assertTrue(list(self.silver.glob(f"{entity}_*.csv")))

    def test_missing_identifiers_are_not_collapsed(self):
        self.write_json(
            "instagram_post_20260101_000000.json",
            [
                {"description": "first"},
                {"description": "second"},
            ],
        )

        posts = self.transformer().process_posts()

        self.assertEqual(len(posts), 2)

    def test_repeated_processor_calls_reset_internal_state(self):
        self.write_json(
            "youtube_video_20260101_000000.json",
            [{"video_id": "video-1"}],
        )
        transformer = self.transformer()

        first = transformer.process_videos()
        second = transformer.process_videos()

        self.assertEqual(len(first), 1)
        self.assertEqual(len(second), 1)

    def test_run_parses_each_file_once(self):
        self.write_json(
            "instagram_profile_20260101_000000.json",
            [{"id": "profile-1", "account": "creator"}],
        )
        transformer = self.transformer()
        transformer.parse_json = Mock(wraps=transformer.parse_json)

        transformer.run()

        self.assertEqual(transformer.parse_json.call_count, 1)

    def test_malformed_json_is_reported_without_stopping_outputs(self):
        path = self.bronze / "instagram_profile_broken.json"
        with path.open("w", encoding="utf-8") as file:
            file.write("{not-json")

        outputs = self.transformer().run()

        self.assertEqual(len(outputs["errors"]), 1)
        self.assertEqual(outputs["errors"][0]["stage"], "parse")
        self.assertTrue(outputs["profiles"].empty)


if __name__ == "__main__":
    unittest.main()
