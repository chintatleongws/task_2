import json
from pathlib import Path
import pandas as pd
from datetime import datetime

class Transformation:
    def __init__(self, base_folder_path="./data/bronze/raw_json"):
        self.bronze_folder = Path(base_folder_path)
        self.silver_folder = Path("./data/silver")
        self.posts = []
        self.comments = []
        self.videos = []
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        

    def parse_json(self, file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
        
    def facebook_reactions(self, item):
        reactions = {
            "impressions_like": None,
            "impressions_love": None,
            "impressions_care": None,
            "impressions_wow": None,
            "impressions_haha": None,
            "impressions_angry": None,
            "impressions_sad": None,
        }
        
        for reaction in item.get("num_likes_type") or []:
            reaction_type = reaction.get("type", "").lower()
            count = reaction.get("num")

            col = f"impressions_{reaction_type}"
            if col in reactions:
                reactions[col] = count
                
        return reactions

    def extract_posts(self, data, source_file):
        # JSON files usually contain a list of records
        if not isinstance(data, list):
            data = [data]

        for item in data:
            source_file_lower = source_file.lower()
            reactions = self.facebook_reactions(item)

            # FACEBOOK POST
            if "fb_post" in source_file_lower:
                self.posts.append({
                    "platform": "facebook",
                    "post_id": item.get("post_id"),
                    "post_url": item.get("url"),
                    "author_id": item.get("profile_id"),
                    "author_name": item.get("user_username_raw"),
                    "author_handle": item.get("user_handle"),
                    "author_url": item.get("user_url") or item.get("page_url"),
                    "content": item.get("content"),
                    "date_posted": item.get("date_posted"),
                    "impressions_total": item.get("num_impressions"),
                    "impressions_upvotes": None,
                    **reactions,
                    "comments_total": item.get("num_comments"),
                    "shares_total": item.get("num_shares"),
                    "post_type": item.get("post_type"),
                    "is_sponsored": item.get("is_sponsored"),
                    "community_name": item.get("community_name"),
                    "source_file": source_file
                })

            # INSTAGRAM POST
            elif "ig_post" in source_file_lower:
                self.posts.append({
                    "platform": "instagram",
                    "post_id": item.get("post_id") or item.get("pk"),
                    "post_url": item.get("url"),
                    "author_id": item.get("user_posted_id"),
                    "author_name": item.get("user_posted"),
                    "author_handle": item.get("user_posted"),
                    "author_url": item.get("profile_url"),
                    "content": item.get("description"),
                    "date_posted": item.get("date_posted"),
                    
                    # Impressions
                    "impressions_total": item.get("likes"),
                    "impressions_like": item.get("likes"),

                    # Facebook-only reactions
                    "impressions_love": None,
                    "impressions_care": None,
                    "impressions_wow": None,
                    "impressions_haha": None,
                    "impressions_angry": None,
                    "impressions_sad": None,

                    # Reddit-only
                    "impressions_upvotes": None,
                    
                    "comments_total": item.get("num_comments"),
                    "shares_total": None,
                    "post_type": item.get("content_type"),
                    "is_sponsored": item.get("is_paid_partnership"),
                    "community_name": item.get("community_name"),
                    "source_file": source_file
                    
                })

            # REDDIT POST
            elif "re_post" in source_file_lower:
                self.posts.append({
                    "platform": "reddit",
                    "post_id": item.get("post_id"),
                    "post_url": item.get("url"),
                    "author_id": item.get("user_id"),
                    "author_name": item.get("user_posted"),
                    "author_handle": item.get("user_posted"),
                    "author_url": None,
                    "content": item.get("description"),
                    "title": item.get("title"),
                    "date_posted": item.get("date_posted"),
                    
                    "impressions_total": item.get("num_upvotes"),
                    "impressions_upvotes": item.get("num_upvotes"),

                    "impressions_like": None,
                    "impressions_love": None,
                    "impressions_care": None,
                    "impressions_wow": None,
                    "impressions_haha": None,
                    "impressions_angry": None,
                    "impressions_sad": None,
                    
                    "comments_total": item.get("num_comments"),
                    "shares_total": None,
                    "post_type": item.get("post_type"),
                    "community_name": item.get("community_name"),
                    "source_file": source_file
                })

    def extract_comments(self, data, source_file):
        """Normalize comments from all supported platforms."""

        if not isinstance(data, list):
            data = [data]

        source_file_lower = source_file.lower()

        for item in data:

            # FACEBOOK COMMENTS
            if (
                "facebook_comment" in source_file_lower
                or "fb_comment" in source_file_lower
            ):
                self.comments.append({
                    "platform": "facebook",
                    "comment_id": item.get("comment_id"),
                    "comment_url": item.get("comment_link"),

                    "post_id": item.get("post_id"),
                    "post_url": item.get("post_url") or item.get("url"),

                    "author_id": item.get("user_id"),
                    "author_name": item.get("user_name"),
                    "author_handle": None,
                    "author_url": (
                        item.get("user_url")
                        or item.get("commentator_profile")
                    ),

                    "comment_text": item.get("comment_text"),
                    "date_posted": item.get("date_created"),

                    "engagement_likes": item.get("num_likes"),
                    "replies_total": item.get("num_replies"),

                    "parent_comment_id": item.get("parent_comment_id"),
                    "root_comment_id": None,

                    "is_reply": bool(
                        item.get("reply")
                        or item.get("parent_comment_id")
                    ),

                    "source_file": source_file,
                })

            # INSTAGRAM COMMENTS
            elif (
                "instagram_comment" in source_file_lower
                or "ig_comment" in source_file_lower
            ):
                self.comments.append({
                    "platform": "instagram",
                    "comment_id": item.get("comment_id"),
                    "comment_url": None,

                    "post_id": None,
                    "post_url": item.get("post_url"),

                    "author_id": None,
                    "author_name": item.get("comment_user"),
                    "author_handle": item.get("comment_user"),
                    "author_url": item.get("comment_user_url"),

                    "comment_text": item.get("comment"),
                    "date_posted": item.get("comment_date"),

                    "engagement_likes": item.get("likes_number"),
                    "replies_total": item.get("replies_number"),

                    "parent_comment_id": None,
                    "root_comment_id": None,
                    "is_reply": False,

                    "source_file": source_file,
                })

            # TIKTOK COMMENTS
            elif (
                "tiktok_comment" in source_file_lower
                or "tt_comment" in source_file_lower
            ):
                comment_id = item.get("comment_id")

                self.comments.append({
                    "platform": "tiktok",
                    "comment_id": comment_id,
                    "comment_url": item.get("comment_url"),

                    "post_id": item.get("post_id"),
                    "post_url": item.get("post_url") or item.get("url"),

                    "author_id": item.get("commenter_id"),
                    "author_name": item.get("commenter_user_name"),
                    "author_handle": self.extract_handle(
                        item.get("commenter_url")
                    ),
                    "author_url": item.get("commenter_url"),

                    "comment_text": item.get("comment_text"),
                    "date_posted": item.get("date_created"),

                    "engagement_likes": item.get("num_likes"),
                    "replies_total": item.get("num_replies"),

                    "parent_comment_id": None,
                    "root_comment_id": comment_id,
                    "is_reply": False,

                    "source_file": source_file,
                })

                # Normalize nested TikTok replies as comment rows.
                for reply in item.get("replies") or []:
                    reply_id = (
                        reply.get("comment_id")
                        or reply.get("reply_id")
                    )

                    self.comments.append({
                        "platform": "tiktok",
                        "comment_id": reply_id,
                        "comment_url": reply.get("comment_url"),

                        "post_id": item.get("post_id"),
                        "post_url": (
                            item.get("post_url")
                            or item.get("url")
                        ),

                        "author_id": (
                            reply.get("commenter_id")
                            or reply.get("user_id")
                        ),
                        "author_name": (
                            reply.get("commenter_user_name")
                            or reply.get("user_name")
                            or reply.get("user_replying")
                        ),
                        "author_handle": self.extract_handle(
                            reply.get("commenter_url")
                            or reply.get("user_url")
                        ),
                        "author_url": (
                            reply.get("commenter_url")
                            or reply.get("user_url")
                        ),

                        "comment_text": (
                            reply.get("comment_text")
                            or reply.get("reply")
                        ),
                        "date_posted": (
                            reply.get("date_created")
                            or reply.get("date_of_reply")
                        ),

                        "engagement_likes": (
                            reply.get("num_likes")
                            if reply.get("num_likes") is not None
                            else reply.get("num_upvotes")
                        ),
                        "replies_total": reply.get("num_replies"),

                        "parent_comment_id": comment_id,
                        "root_comment_id": comment_id,
                        "is_reply": True,

                        "source_file": source_file,
                    })

            # REDDIT COMMENTS
            elif (
                "reddit_comment" in source_file_lower
                or "re_comment" in source_file_lower
            ):
                comment_id = item.get("comment_id")

                root_id = self.clean_reddit_id(
                    item.get("root_comment_id")
                )

                post_id = item.get("post_id") or root_id

                post_url = (
                    item.get("post_url")
                    or (item.get("input") or {}).get("url")
                )

                self.comments.append({
                    "platform": "reddit",
                    "comment_id": comment_id,
                    "comment_url": item.get("url"),

                    "post_id": post_id,
                    "post_url": post_url,

                    "author_id": None,
                    "author_name": item.get("user_posted"),
                    "author_handle": item.get("user_posted"),
                    "author_url": self.reddit_user_url(
                        item.get("user_posted")
                    ),

                    "comment_text": item.get("comment"),
                    "date_posted": item.get("date_posted"),

                    "engagement_likes": item.get("num_upvotes"),
                    "replies_total": item.get("num_replies"),

                    "parent_comment_id": self.clean_reddit_id(
                        item.get("parent_comment_id")
                    ),
                    "root_comment_id": root_id,

                    "is_reply": False,

                    "source_file": source_file,
                })

                # Normalize nested Reddit replies as comment rows.
                for reply in item.get("replies") or []:
                    reply_id = reply.get("reply_id")

                    self.comments.append({
                        "platform": "reddit",
                        "comment_id": reply_id,
                        "comment_url": None,

                        "post_id": post_id,
                        "post_url": post_url,

                        "author_id": None,
                        "author_name": reply.get("user_replying"),
                        "author_handle": reply.get("user_replying"),
                        "author_url": reply.get("user_url"),

                        "comment_text": reply.get("reply"),
                        "date_posted": reply.get("date_of_reply"),

                        "engagement_likes": reply.get("num_upvotes"),
                        "replies_total": reply.get("num_replies"),

                        "parent_comment_id": comment_id,
                        "root_comment_id": root_id or comment_id,
                        "is_reply": True,

                        "source_file": source_file,
                    })
                    
    def extract_videos(self, data, source_file):
        """ Normalize videos from all supported platforms."""
        if not isinstance(data, list):
            data = [data]

        for item in data:
            source_file_lower = source_file.lower()

            # TIKTOK VIDEO
            if (
                "tiktok_video" in source_file_lower
                or "tt_video" in source_file_lower
                or "tiktok_post" in source_file_lower
            ):
                music = item.get("music") or {}

                self.videos.append({
                    "platform": "tiktok",

                    "video_id": (
                        item.get("post_id")
                        or item.get("shortcode")
                    ),
                    "post_url": item.get("url"),
                    "media_url": (
                        item.get("video_url")
                        or item.get("cdn_link")
                        or item.get("cdn_url")
                    ),

                    "author_id": item.get("profile_id"),
                    "author_name": item.get("profile_username"),
                    "author_handle": (
                        item.get("account_id")
                        or self.extract_handle(
                            item.get("profile_url")
                        )
                    ),
                    "author_url": item.get("profile_url"),

                    "title": None,
                    "description": item.get("description"),
                    "date_posted": item.get("create_time"),
                    "duration_seconds": item.get("video_duration"),

                    "views_total": item.get("play_count"),
                    "likes_total": item.get("digg_count"),
                    "comments_total": item.get("comment_count"),
                    "shares_total": (
                        item.get("share_count")
                        or item.get("num_share_count")
                    ),
                    "saves_total": item.get("collect_count"),

                    "audio_title": (
                        music.get("title")
                        or item.get("original_sound")
                    ),
                    "audio_artist": music.get("authorname"),

                    "post_type": item.get("post_type"),
                    "is_verified": item.get("is_verified"),
                    "is_sponsored": self._has_commerce_info(
                        item.get("commerce_info")
                    ),

                    "source_file": source_file,
                })

            # INSTAGRAM VIDEO / REEL
            elif (
                "instagram_video" in source_file_lower
                or "ig_video" in source_file_lower
                or "instagram_reel" in source_file_lower
                or "ig_reel" in source_file_lower
            ):
                self.videos.append({
                    "platform": "instagram",

                    "video_id": (
                        item.get("post_id")
                        or item.get("shortcode")
                    ),
                    "post_url": item.get("url"),
                    "media_url": item.get("video_url"),

                    "author_id": item.get("user_posted_id"),
                    "author_name": item.get("user_posted"),
                    "author_handle": item.get("user_posted"),
                    "author_url": (
                        item.get("user_profile_url")
                        or item.get("profile_url")
                    ),

                    "title": None,
                    "description": item.get("description"),
                    "date_posted": item.get("date_posted"),
                    "duration_seconds": item.get("length"),

                    "views_total": (
                        item.get("views")
                        if item.get("views") is not None
                        else item.get("video_play_count")
                    ),
                    "likes_total": item.get("likes"),
                    "comments_total": item.get("num_comments"),
                    "shares_total": None,
                    "saves_total": None,

                    "audio_title": None,
                    "audio_artist": None,

                    "post_type": (
                        item.get("product_type")
                        or "reel"
                    ),
                    "is_verified": item.get("is_verified"),
                    "is_sponsored": item.get(
                        "is_paid_partnership"
                    ),

                    "source_file": source_file,
                })

            # YOUTUBE VIDEO
            elif (
                "youtube_video" in source_file_lower
                or "yt_video" in source_file_lower
            ):
                music = item.get("music") or {}

                self.videos.append({
                    "platform": "youtube",

                    "video_id": (
                        item.get("video_id")
                        or item.get("shortcode")
                    ),
                    "post_url": item.get("url"),
                    "media_url": item.get("video_url"),

                    "author_id": item.get("youtuber_id"),
                    "author_name": (
                        item.get("handle_name")
                        or item.get("youtuber")
                    ),
                    "author_handle": item.get("youtuber"),
                    "author_url": (
                        item.get("channel_url")
                        or item.get("channel_url_decoded")
                    ),

                    "title": item.get("title"),
                    "description": item.get("description"),
                    "date_posted": item.get("date_posted"),
                    "duration_seconds": item.get("video_length"),

                    "views_total": item.get("views"),
                    "likes_total": item.get("likes"),
                    "comments_total": item.get("num_comments"),
                    "shares_total": None,
                    "saves_total": None,

                    "audio_title": music.get("song"),
                    "audio_artist": music.get("artist"),

                    "post_type": item.get("post_type"),
                    "is_verified": item.get("verified"),
                    "is_sponsored": item.get("is_sponsored"),

                    "source_file": source_file,
                })
            
        
        
    @staticmethod
    def extract_handle(profile_url):
        if not profile_url:
            return None

        return profile_url.rstrip("/").split("/")[-1].lstrip("@")


    @staticmethod
    def clean_reddit_id(value):
        if not value:
            return None

        if "_" in value:
            return value.split("_", 1)[1]

        return value


    @staticmethod
    def reddit_user_url(username):
        if not username or username == "[deleted]":
            return None

        return f"https://www.reddit.com/user/{username}/"
    
    @staticmethod
    def _has_commerce_info(commerce_info):
        if commerce_info is None:
            return False

        if isinstance(commerce_info, dict):
            return bool(commerce_info)

        if isinstance(commerce_info, list):
            return len(commerce_info) > 0

        return bool(commerce_info)
    
    def process_posts(self):
        for file in self.bronze_folder.glob("*.json"):
            data = self.parse_json(file)
            self.extract_posts(data, file.name)
        posts_df = pd.DataFrame(self.posts)
        posts_df = posts_df.drop_duplicates(subset=["platform", "post_id"])

        output_path = self.silver_folder / f"posts_{self.timestamp}.csv"
        posts_df.to_csv(output_path, index=False)
        print(f"Saved posts_{self.timestamp}.csv at {output_path}")

        return posts_df
    
    def process_comments(self):
        for file in self.bronze_folder.glob("*.json"):
            data = self.parse_json(file)
            self.extract_comments(data, file.name)
        comments_df = pd.DataFrame(self.comments)
        comments_df = comments_df.drop_duplicates(subset=["platform", "comment_id"])
        
        self.silver_folder.mkdir(parents=True, exist_ok=True)
        
        output_path = self.silver_folder / f"comments_{self.timestamp}.csv"

        comments_df.to_csv(output_path, index=False)
        print(f"Saved comments_{self.timestamp}.csv at {output_path}")

        return comments_df
    
    def process_videos(self):
        for file in self.bronze_folder.glob("*.json"):
            try:
                data = self.parse_json(file)
                self.extract_videos(data, file.name)

            except (json.JSONDecodeError, OSError) as error:
                print(
                    f"Could not process video file "
                    f"{file.name}: {error}"
                )

        videos_df = pd.DataFrame(self.videos)
        videos_df = videos_df.drop_duplicates(
            subset=["platform", "video_id"],
            keep="last",
        )

        videos_df["date_posted"] = pd.to_datetime(
            videos_df["date_posted"],
            errors="coerce",
            utc=True,
        )

        boolean_columns = [
            "is_verified",
            "is_sponsored",
        ]

        for column in boolean_columns:
            videos_df[column] = videos_df[column].astype(
                "boolean"
            )

        self.silver_folder.mkdir(
            parents=True,   # create parent directories if they don't exist
            exist_ok=True,  # do not raise an error if the directory already exists
        )

        output_path = self.silver_folder / f"videos_{self.timestamp}.csv"

        videos_df.to_csv(
            output_path,
            index=False,
        )

        print(
            f"Saved videos_{self.timestamp}.csv at {output_path}"
        )

        return videos_df
    
    def run(self):
        posts_df = self.process_posts()
        comments_df = self.process_comments()
        videos_df = self.process_videos()

        return videos_df


# test the Transformation class
if __name__ == "__main__":
    transformer = Transformation()
    videos_df = transformer.run()

    print(videos_df.head().to_string(index=False))

 