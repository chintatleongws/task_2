# Scraper and Transformation Guide

This document describes the two core stages of the social-media ELT pipeline:

1. `BrightDataAPI` in `scraper_class.py` extracts raw records from Bright Data.
2. `Transformation` in `transformation.py` normalizes the raw JSON into CSV datasets.

The guide documents the code as it currently behaves, including implementation
constraints that are useful when maintaining or extending it.

## Architecture

```text
Social-media URLs
        |
        v
BrightDataAPI
  - identifies platform and dataset
  - submits Bright Data requests
  - polls snapshots when required
  - separates successful and error records
        |
        v
data/bronze/raw_json/*.json
        |
        v
Transformation
  - identifies record type from the filename
  - maps platform-specific fields to shared schemas
  - de-duplicates and converts selected data types
        |
        v
data/silver/{posts,comments,videos,profiles}_<timestamp>.csv
```

The bronze layer preserves source-shaped data. The silver layer provides common
cross-platform columns suitable for downstream analytics.

## Requirements and configuration

The project requires Python 3.12 or newer within the range declared in
`pyproject.toml.`

Install dependencies with Poetry:

```bash
poetry install
```

The scraper reads the following environment variables:

| Variable | Required | Default | Purpose |
|---|---:|---|---|
| `BRIGHTDATA_API_KEY` | Yes | None | Bearer token used for Bright Data requests |
| `STORAGE_MODE` | No | `local` | Selects `local`, `s3`, `gcs`, or `azure` |

For local use, a `.env` file can contain:

```dotenv
BRIGHTDATA_API_KEY=your-api-key
STORAGE_MODE=local
```

Do not commit real API keys.

Although cloud storage modes are declared, `_save_result()` currently uses
`os.makedirs()` and Python's local `open()`. Only `STORAGE_MODE=local` is
implemented end to end.

## Scraper: `BrightDataAPI`

### Responsibility

`BrightDataAPI` is an adapter around the Bright Data Datasets API. It translates
application-level requests such as “scrape these profile URLs” into dataset IDs,
HTTP requests, snapshot polling, and locally stored JSON.

The class is also a facade: callers can use one high-level `scrape()` operation
without knowing Bright Data dataset IDs or snapshot endpoints.

### Construction

```python
from scraper_class import BrightDataAPI

api = BrightDataAPI(
    api_key="optional-explicit-key",
    max_retries=3,
    retry_delay=2.0,
)
```

If `api_key` is omitted, the constructor reads `BRIGHTDATA_API_KEY`. It raises
`ValueError` if neither is available.

The constructor creates:

- The Bright Data base URL.
- Authorization and content-type headers.
- Retry configuration.
- A mapping of internal dataset keys to Bright Data dataset IDs.

### Supported dataset routes

The generic `scrape()` method combines a platform with an entity:

| Platform | Profile/channel | Post | Video | Comment |
|---|---|---|---|---|
| Instagram | `instagram_profile` | `instagram_post` | `instagram_video` | `instagram_comment` |
| Facebook | `facebook_profile` | `facebook_post` | `facebook_post` | `facebook_comment` |
| TikTok | `tiktok_profile` | `tiktok_video` | `tiktok_video` | `tiktok_comment` |
| YouTube | `youtube_channel` | `youtube_video` | `youtube_video` | `youtube_comments` |
| Reddit | Not supported | `reddit_post` | Not supported | `reddit_comments` |

Accepted entity aliases are singular or plural forms of:

- `profile`
- `channel`
- `post`
- `video`
- `comment`

`channel` is only routed for YouTube. Unsupported platform/entity combinations
are rejected by the generic interface.

### Recommended interface: `scrape()`

```python
from scraper_class import BrightDataAPI

api = BrightDataAPI()

result = api.scrape(
    urls=[
        "https://www.youtube.com/user/whitehouse",
        "https://www.instagram.com/zuck",
    ],
    entity="profile",
)

print(result)
```

All URLs in one call must represent the requested entity type, but they may come
from different platforms. For example, profile URLs from Instagram and YouTube
can share a call; a profile URL and a comment URL should not.

`scrape()` performs the following operations:

1. Normalizes the entity name.
2. Infers the platform of every URL.
3. Resolves each platform/entity pair to a dataset key.
4. Groups URLs that use the same dataset.
5. Runs each dataset group.
6. Returns successful group results and rejected URLs.

An empty URL list returns empty `results` and `rejected` collections.

### Successful response shape

A successful Bright Data record contains the scraped source fields and does not
contain `error` or `error_code`. For example, a Facebook profile record normally
has a shape similar to:

```json
{
  "url": "https://www.facebook.com/zuck/",
  "name": "Mark Zuckerberg",
  "id": "4",
  "profile_photo": "https://example.com/profile-photo.jpg",
  "cover_photo": "https://example.com/cover-photo.jpg",
  "work": null,
  "college": null,
  "high_school": null,
  "photos": [
    "https://example.com/photo-1.jpg"
  ],
  "location": "Palo Alto, California",
  "city": "Palo Alto",
  "timestamp": "2026-07-21T14:13:16.751Z",
  "input": {
    "url": "https://www.facebook.com/zuck/"
  }
}
```

`_split_results()` recognizes this as successful because neither error field is
present. It places the object in `valid_records`.

The generic `scrape()` method then wraps the successful Bright Data record with
application metadata:

```json
{
  "entity": "profile",
  "results": {
    "facebook_profile": {
      "status": "success",
      "urls": [
        "https://www.facebook.com/zuck/"
      ],
      "records": {
        "records": [
          {
            "url": "https://www.facebook.com/zuck/",
            "name": "Mark Zuckerberg",
            "id": "4",
            "location": "Palo Alto, California",
            "input": {
              "url": "https://www.facebook.com/zuck/"
            }
          }
        ],
        "errors": [],
        "records_count": 1,
        "errors_count": 0
      }
    }
  },
  "rejected": []
}
```

The empty `errors` and `rejected` collections are added by this project's
adapter; they are not fields in the successful Bright Data record.

The outer `records` property is the result envelope returned by `_run()`.
Consequently, the successful Bright Data records are found at:

```python
result["results"][dataset_key]["records"]["records"]
```

An `error` string is only added to a dataset group when that complete group
fails. Per-record Bright Data failures are placed in the adapter's `errors`
collection. A failure in one group does not discard successful groups.

### Public interface

`scrape()` is the single public scraping interface. Platform-specific
convenience wrappers were removed because they duplicated its routing behavior.
Use the `entity` argument to request profiles, channels, posts, videos, or
comments; platform detection and dataset selection happen automatically.

### Request processing

`_run()` passes each dataset group to `_submit_scrape()`. That method submits
the URL payload and handles either possible Bright Data response:

- Immediate records are returned directly.
- A response containing `snapshot_id` is polled until its records are ready.

The interface is synchronous and blocking. It uses `requests` for HTTP and
`time.sleep()` between snapshot progress checks.

### Snapshot lifecycle

For snapshot-backed requests:

1. `_submit_scrape()` submits the URL payload and receives `snapshot_id`.
2. `_monitor_snapshot()` calls the progress endpoint.
3. `_wait_for_snapshot()` polls every five seconds.
4. `_download_snapshot()` retrieves JSON when the job is ready.

The accepted ready statuses are `ready`, `completed`, and `done`. The accepted
failure statuses are `failed` and `error`. Polling times out after 900 seconds by
default.

### Record validation and bronze storage

`_split_results()` classifies every returned item:

- A dictionary without `error` or `error_code` is valid.
- A dictionary with either error field is an error record.
- A non-dictionary becomes an error with `Unexpected response type`.

Only valid records are saved. `_run()` still returns both collections and their
counts.

For local storage, all raw files are written directly to
`data/bronze/raw_json`. File names follow:

```text
<dataset_key>_<YYYYMMDD_HHMMSS>.json
```

Posts, comments, videos, and profiles share this flat bronze directory. The
dataset key in the filename identifies the record type; there are no separate
post or comment subdirectories.

### Job payload helper

`build_job_payload()` creates a queue-friendly description of a scrape:

```python
payload = api.build_job_payload(
    urls=["https://www.instagram.com/reel/example"],
    dataset_key="instagram_post",
    job_id="job_instagram_001",
)
```

Result:

```json
{
  "id": "job_instagram_001",
  "source": "brightdata",
  "data": {
    "dataset": "instagram_post",
    "dataset_id": "configured-bright-data-id",
    "urls": ["https://www.instagram.com/reel/example"]
  }
}
```

If the dataset key is omitted, it is inferred from the first URL. This inference
is less precise than the platform/entity routing in `scrape()`:

- Every YouTube URL is inferred as `youtube_video`.
- Every TikTok URL is inferred as `tiktok_video`.
- Every Reddit URL is inferred as `reddit_post`.

Use an explicit `dataset_key` for channels, profiles, or comments.

### Scraper exceptions

| Exception | Meaning |
|---|---|
| `ValueError` | Missing API key, unsupported URL/platform/entity, or unknown dataset |
| `BrightDataRequestError` | Bright Data HTTP request failed |
| `RuntimeError` | Snapshot reported a failed state |
| `TimeoutError` | Snapshot did not become ready within the polling limit |

## Transformation: `Transformation`

### Responsibility

`Transformation` reads raw JSON from the bronze layer and maps source-specific
fields into four cross-platform tables:

- Posts
- Comments
- Videos
- Profiles

It uses pandas for de-duplication, selected type conversions, and CSV output.

### Construction

```python
from transformation import Transformation

transformer = Transformation(
    base_folder_path="./data/bronze/raw_json",
)
```

The input folder can be changed, but the silver output directory is currently
fixed at `./data/silver`.

One timestamp is generated when the object is constructed and reused for all
CSV files produced by that object.

### Filename-driven dispatch

The transformer does not inspect a metadata field to select a parser. It selects
the platform and entity by searching the source filename.

This is a critical input contract:

| Entity | Recognized filename fragments |
|---|---|
| Facebook posts | `fb_post` |
| Instagram posts | `ig_post` |
| Reddit posts | `re_post` |
| Facebook comments | `facebook_comment`, `fb_comment` |
| Instagram comments | `instagram_comment`, `ig_comment` |
| TikTok comments | `tiktok_comment`, `tt_comment` |
| Reddit comments | `reddit_comment`, `re_comment` |
| TikTok videos | `tiktok_video`, `tt_video`, `tiktok_post` |
| Instagram videos | `instagram_video`, `ig_video`, `instagram_reel`, `ig_reel` |
| YouTube videos | `youtube_video`, `yt_video` |
| Facebook profiles | `facebook_profile`, `fb_profile` |
| Instagram profiles | `instagram_profile`, `ig_profile` |
| TikTok profiles | `tiktok_profile`, `tt_profile` |
| YouTube profiles | `youtube_channel`, `youtube_profile`, `yt_channel` |

Records from unrecognized filenames are silently ignored by the corresponding
extractor.

### Running all transformations

From the command line:

```bash
poetry run python transformation.py
```

Or from Python:

```python
transformer = Transformation()
profiles_df = transformer.run()
```

`run()` calls, in order:

1. `process_posts()`
2. `process_comments()`
3. `process_videos()`
4. `process_profiles()`

All four CSVs are written, but the method currently returns only the profiles
DataFrame.

Individual entity pipelines can also be called:

```python
posts_df = transformer.process_posts()
comments_df = transformer.process_comments()
videos_df = transformer.process_videos()
profiles_df = transformer.process_profiles()
```

When calling an individual processor repeatedly on the same object, note that
posts, comments, and videos are not reset at the beginning of processing.
`process_profiles()` does reset `self.profiles`.

### Input discovery

The processors do not all scan directories in the same way:

| Processor | File search |
|---|---|
| Posts | `bronze_folder.glob("*.json")` |
| Comments | `bronze_folder.glob("*.json")` |
| Videos | `bronze_folder.glob("*.json")` |
| Profiles | `bronze_folder.rglob("*.json")` |

Only profiles search nested directories. Posts, comments, and videos read JSON
files immediately inside the bronze directory. With the project's flat bronze
layout, all four processors can discover their input files.

### Posts

`extract_posts()` supports Facebook, Instagram, and Reddit.

The normalized post schema is:

| Column group | Columns |
|---|---|
| Identity | `platform`, `post_id`, `post_url` |
| Author | `author_id`, `author_name`, `author_handle`, `author_url` |
| Content | `content`, `title`, `date_posted`, `post_type` |
| Engagement | `impressions_total`, `impressions_upvotes`, `impressions_like`, `impressions_love`, `impressions_care`, `impressions_wow`, `impressions_haha`, `impressions_angry`, `impressions_sad`, `comments_total`, `shares_total` |
| Context | `is_sponsored`, `community_name`, `source_file` |

Facebook's `num_likes_type` collection is expanded by
`facebook_reactions()` into individual reaction columns.

Posts are de-duplicated by `(platform, post_id)`. The default pandas behavior
keeps the first duplicate.

### Comments

`extract_comments()` supports Facebook, Instagram, TikTok, and Reddit.

The normalized comment schema is:

| Column group | Columns |
|---|---|
| Identity | `platform`, `comment_id`, `comment_url` |
| Parent post | `post_id`, `post_url` |
| Author | `author_id`, `author_name`, `author_handle`, `author_url` |
| Content | `comment_text`, `date_posted` |
| Engagement | `engagement_likes`, `replies_total` |
| Threading | `parent_comment_id`, `root_comment_id`, `is_reply` |
| Lineage | `source_file` |

Nested TikTok and Reddit replies are flattened into their own comment rows.
Their parent and root identifiers preserve thread relationships where source
data makes those identifiers available.

Comments are de-duplicated by `(platform, comment_id)`, keeping the first
duplicate.

### Videos

`extract_videos()` supports TikTok, Instagram/Reels, and YouTube.

The normalized video schema is:

| Column group | Columns |
|---|---|
| Identity | `platform`, `video_id`, `post_url`, `media_url` |
| Author | `author_id`, `author_name`, `author_handle`, `author_url` |
| Content | `title`, `description`, `date_posted`, `duration_seconds`, `post_type` |
| Engagement | `views_total`, `likes_total`, `comments_total`, `shares_total`, `saves_total` |
| Audio | `audio_title`, `audio_artist` |
| Flags | `is_verified`, `is_sponsored` |
| Lineage | `source_file` |

Videos are de-duplicated by `(platform, video_id)`, keeping the last duplicate.
`date_posted` is converted to a UTC-aware pandas datetime, while `is_verified`
and `is_sponsored` are converted to pandas' nullable Boolean type.

### Profiles

`extract_profiles()` supports Facebook, Instagram, TikTok, and YouTube
channels.

The normalized profile schema is:

| Column group | Columns |
|---|---|
| Identity | `platform`, `profile_id`, `profile_url`, `profile_handle`, `profile_name` |
| Description | `biography`, `location`, `website_url`, `created_date` |
| Audience/content metrics | `followers_total`, `following_total`, `friends_total`, `subscribers_total`, `posts_total`, `videos_total`, `likes_total`, `views_total`, `engagement_rate` |
| Flags | `is_verified`, `is_private`, `is_business`, `is_professional` |
| Lineage | `source_file` |

Profiles are de-duplicated by `(platform, profile_id)`, keeping the last
duplicate.

Metric columns are converted to nullable `Int64`, `engagement_rate` is converted
to numeric, and `created_date` becomes a UTC-aware pandas datetime. Profile
flags use pandas' nullable Boolean type.

### Transformation helper methods

| Method | Purpose |
|---|---|
| `parse_json()` | Loads one UTF-8 JSON document |
| `facebook_reactions()` | Expands Facebook reaction counts into columns |
| `extract_handle()` | Takes the final URL path component and removes leading `@` |
| `clean_reddit_id()` | Removes prefixes such as `t1_` from Reddit IDs |
| `reddit_user_url()` | Builds a Reddit profile URL unless the user is deleted |
| `_has_commerce_info()` | Converts TikTok commerce information into a sponsorship Boolean |

### Silver outputs

Output files use the timestamp created with the transformer:

```text
data/silver/posts_<YYYYMMDD_HHMMSS>.csv
data/silver/comments_<YYYYMMDD_HHMMSS>.csv
data/silver/videos_<YYYYMMDD_HHMMSS>.csv
data/silver/profiles_<YYYYMMDD_HHMMSS>.csv
```

Every normalized table includes `source_file` for bronze-to-silver lineage.

## End-to-end example

The following example scrapes Instagram profiles and then transforms all
available bronze data:

```python
from scraper_class import BrightDataAPI
from transformation import Transformation


api = BrightDataAPI()
scrape_summary = api.scrape(
    urls=["https://www.instagram.com/zuck"],
    entity="profile",
)

transformer = Transformation()
profiles_df = transformer.process_profiles()

print(scrape_summary)
print(profiles_df.head())
```

The scraper writes an `instagram_profile_<timestamp>.json` file. That filename
is recognized by `extract_profiles()`, which produces an Instagram row in the
profiles CSV.

## Current integration boundaries

The repository also contains Airflow, SQS/LocalStack, worker, and Lambda-like
components. Their current behavior is:

- `airflow_scheduler.py` creates scrape descriptions with
  `build_job_payload()` and publishes them to SQS.
- `worker.py` consumes queue messages and passes them to
  `utils.process_message_body()`.
- `process_message_body()` records a `status: "processed"` copy in
  S3/LocalStack.
- The worker does not currently instantiate `BrightDataAPI` or execute a Bright
  Data scrape.
- `transformation.py` reads local bronze JSON directly; it does not read the
  S3/LocalStack processed payloads.

The direct local path (`scraper_class.py` followed by `transformation.py`) and
the queue path are therefore not yet one continuous end-to-end pipeline.

## Known limitations

These points describe current behavior and are useful targets for future work:

1. **Post filename mismatch.** The scraper saves keys such as
   `facebook_post`, `instagram_post`, and `reddit_post`, while
   `extract_posts()` only recognizes `fb_post`, `ig_post`, and `re_post`.
2. **Sequential dataset groups.** Mixed-platform dataset groups are processed
   one at a time. There is no concurrent HTTP execution.
3. **Partial retry behavior.** Explicit non-success HTTP responses raise
   `BrightDataRequestError`, which is not caught by the retry handler that only
   catches `requests.RequestException`.
4. **JSON fallback behavior.** The JSON decoding exception handler calls
   `response.json()` a second time before attempting line-by-line parsing.
5. **Cloud paths are placeholders.** The save implementation uses local
   filesystem functions for every storage mode.
6. **Possible file collisions.** Bronze filenames have one-second timestamp
   precision, so same-dataset writes within one second can overwrite a file.
7. **Empty transformation edge cases.** Some processors assume expected columns
   exist. If no matching records are extracted, de-duplication or type
   conversion can raise `KeyError`.
8. **`run()` returns one table.** All four entity CSVs are created, but only the
    profiles DataFrame is returned.

## Extension checklist

When adding a platform or entity:

1. Add the Bright Data dataset key and ID to `BrightDataAPI.datasets`.
2. Add the platform/entity route to `resolve_dataset_key()`.
3. Define the bronze filename convention.
4. Add a matching branch in the relevant `extract_*()` method.
5. Map source fields to the existing silver schema, or document a schema change.
6. Update de-duplication keys and type conversions if required.
7. Test a successful record, an error record, missing optional fields, and an
   empty input.
8. Verify both the bronze JSON path and resulting silver CSV.
