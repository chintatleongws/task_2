import json
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


class Transformation:
    POST_COLUMNS = [
        "platform", "post_id", "post_url", "author_id", "author_name",
        "author_handle", "author_url", "content", "title", "date_posted",
        "impressions_total", "impressions_upvotes", "impressions_like",
        "impressions_love", "impressions_care", "impressions_wow",
        "impressions_haha", "impressions_angry", "impressions_sad",
        "comments_total", "shares_total", "post_type", "is_sponsored",
        "community_name", "source_file",
    ]
    COMMENT_COLUMNS = [
        "platform", "comment_id", "comment_url", "post_id", "post_url",
        "author_id", "author_name", "author_handle", "author_url",
        "comment_text", "date_posted", "engagement_likes", "replies_total",
        "parent_comment_id", "root_comment_id", "is_reply", "source_file",
    ]
    VIDEO_COLUMNS = [
        "platform", "video_id", "post_url", "media_url", "author_id",
        "author_name", "author_handle", "author_url", "title", "description",
        "date_posted", "duration_seconds", "views_total", "likes_total",
        "comments_total", "shares_total", "saves_total", "audio_title",
        "audio_artist", "post_type", "is_verified", "is_sponsored",
        "source_file",
    ]
    PROFILE_COLUMNS = [
        "platform", "profile_id", "profile_url", "profile_handle",
        "profile_name", "biography", "followers_total", "following_total",
        "friends_total", "subscribers_total", "posts_total", "videos_total",
        "likes_total", "views_total", "location", "website_url",
        "created_date", "is_verified", "is_private", "is_business",
        "is_professional", "engagement_rate", "source_file",
    ]

    def __init__(
        self,
        base_folder_path="./data/bronze/raw_json",
        silver_folder_path="./data/silver",
    ):
        self.bronze_folder = Path(base_folder_path)
        self.silver_folder = Path(silver_folder_path)
        self.posts = []
        self.comments = []
        self.videos = []
        self.profiles = []
        self.errors = []
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")

    def parse_json(self, file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
        
    @staticmethod
    def facebook_reactions(item):
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
            if not isinstance(reaction, dict):
                continue

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
            if not isinstance(item, dict):
                continue

            source_file_lower = source_file.lower()
            reactions = self.facebook_reactions(item)

            # FACEBOOK POST
            if (
                "facebook_post" in source_file_lower
                or "fb_post" in source_file_lower
            ):
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
            elif (
                "instagram_post" in source_file_lower
                or "ig_post" in source_file_lower
            ):
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
            elif (
                "reddit_post" in source_file_lower
                or "re_post" in source_file_lower
            ):
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
            if not isinstance(item, dict):
                continue

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
                    if not isinstance(reply, dict):
                        continue

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
                parent_id = self.clean_reddit_id(
                    item.get("parent_comment_id")
                )

                post_id = item.get("post_id") or root_id

                input_data = item.get("input") or {}
                if not isinstance(input_data, dict):
                    input_data = {}

                post_url = (
                    item.get("post_url")
                    or input_data.get("url")
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

                    "parent_comment_id": parent_id,
                    "root_comment_id": root_id,

                    "is_reply": bool(parent_id),

                    "source_file": source_file,
                })

                # Normalize nested Reddit replies as comment rows.
                for reply in item.get("replies") or []:
                    if not isinstance(reply, dict):
                        continue

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

            # YOUTUBE COMMENTS
            elif (
                "youtube_comments" in source_file_lower
                or "youtube_comment" in source_file_lower
                or "yt_comment" in source_file_lower
            ):
                author_url = (
                    item.get("user_channel")
                    or item.get("comment_user_url")
                )

                self.comments.append({
                    "platform": "youtube",
                    "comment_id": item.get("comment_id"),
                    "comment_url": item.get("comment_url"),
                    "post_id": item.get("video_id"),
                    "post_url": item.get("url"),
                    "author_id": item.get("user_id"),
                    "author_name": (
                        item.get("username")
                        or item.get("comment_user")
                    ),
                    "author_handle": self.extract_handle(author_url),
                    "author_url": author_url,
                    "comment_text": (
                        item.get("comment_text")
                        or item.get("comment")
                    ),
                    "date_posted": (
                        item.get("date")
                        or item.get("comment_date")
                    ),
                    "engagement_likes": (
                        item.get("likes")
                        if item.get("likes") is not None
                        else item.get("num_likes")
                    ),
                    "replies_total": (
                        item.get("replies")
                        if item.get("replies") is not None
                        else item.get("num_replies")
                    ),
                    "parent_comment_id": None,
                    "root_comment_id": None,
                    "is_reply": False,
                    "source_file": source_file,
                })
                    
    def extract_videos(self, data, source_file):
        """ Normalize videos from all supported platforms."""
        if not isinstance(data, list):
            data = [data]

        for item in data:
            if not isinstance(item, dict):
                continue

            source_file_lower = source_file.lower()

            # TIKTOK VIDEO
            if (
                "tiktok_video" in source_file_lower
                or "tt_video" in source_file_lower
                or "tiktok_post" in source_file_lower
            ):
                music = item.get("music") or {}
                if not isinstance(music, dict):
                    music = {}

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
                        if item.get("share_count") is not None
                        else item.get("num_share_count")
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
                if not isinstance(music, dict):
                    music = {}

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
                        or item.get("channel_name")
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
                    "duration_seconds": (
                        item.get("video_length")
                        or item.get("duration")
                    ),

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

    def extract_profiles(self, data, source_file):
        """Normalize profiles from all supported platforms."""

        if not isinstance(data, list):
            data = [data]

        source_file_lower = source_file.lower()

        for item in data:
            if not isinstance(item, dict):
                continue

            # FACEBOOK PROFILE
            if (
                "facebook_profile" in source_file_lower
                or "fb_profile" in source_file_lower
            ):
                self.profiles.append({
                    "platform": "facebook",

                    "profile_id": item.get("id"),
                    "profile_url": item.get("url"),
                    "profile_handle": self.extract_handle(
                        item.get("url")
                    ),
                    "profile_name": item.get("name"),
                    "biography": (
                        item.get("biography")
                        or item.get("bio")
                        or item.get("about")
                    ),

                    "followers_total": (            
                        item.get("followers")
                        if item.get("followers") is not None
                        else item.get("followers_count")
                    ),
                    "following_total": (        # following are not scraped for facebook
                        item.get("following")
                        if item.get("following") is not None
                        else item.get("following_count")
                    ),
                    "friends_total": (
                        item.get("friends")
                        if item.get("friends") is not None
                        else item.get("friends_count")
                    ),

                    "subscribers_total": None,
                    "posts_total": item.get("posts_count"),
                    "videos_total": item.get("videos_count"),
                    "likes_total": (
                        item.get("likes")
                        if item.get("likes") is not None
                        else item.get("page_likes")
                    ),
                    "views_total": None,

                    "location": (
                        item.get("location")
                        or item.get("city")
                    ),
                    "website_url": item.get("website"),
                    "created_date": item.get("created_date"),

                    "is_verified": item.get("is_verified"),
                    "is_private": item.get("is_private"),
                    "is_business": item.get("is_business"),
                    "is_professional": item.get(
                        "is_professional"
                    ),

                    "engagement_rate": item.get(
                        "avg_engagement"
                    ),

                    "source_file": source_file,
                })

            # INSTAGRAM PROFILE
            elif (
                "instagram_profile" in source_file_lower
                or "ig_profile" in source_file_lower
            ):
                external_url = item.get("external_url")

                if isinstance(external_url, list):
                    website_url = (
                        external_url[0]
                        if external_url
                        else None
                    )
                else:
                    website_url = external_url

                self.profiles.append({
                    "platform": "instagram",

                    "profile_id": (
                        item.get("id")
                        or item.get("partner_id")
                    ),
                    "profile_url": (
                        item.get("profile_url")
                        or item.get("url")
                    ),
                    "profile_handle": item.get("account"),
                    "profile_name": (
                        item.get("profile_name")
                        or item.get("full_name")
                    ),
                    "biography": item.get("biography"),

                    "followers_total": item.get("followers"),
                    "following_total": item.get("following"),
                    "friends_total": None,

                    "subscribers_total": None,
                    "posts_total": item.get("posts_count"),
                    "videos_total": None,
                    "likes_total": None,
                    "views_total": None,

                    "location": item.get("business_address"),
                    "website_url": website_url,
                    "created_date": None,

                    "is_verified": item.get("is_verified"),
                    "is_private": item.get("is_private"),
                    "is_business": item.get(
                        "is_business_account"
                    ),
                    "is_professional": item.get(
                        "is_professional_account"
                    ),

                    "engagement_rate": item.get(
                        "avg_engagement"
                    ),

                    "source_file": source_file,
                })

            # TIKTOK PROFILE
            elif (
                "tiktok_profile" in source_file_lower
                or "tt_profile" in source_file_lower
            ):
                self.profiles.append({
                    "platform": "tiktok",

                    "profile_id": item.get("id"),
                    "profile_url": item.get("url"),
                    "profile_handle": item.get("account_id"),
                    "profile_name": item.get("nickname"),
                    "biography": (
                        item.get("biography")
                        or item.get("signature")
                    ),

                    "followers_total": item.get("followers"),
                    "following_total": item.get("following"),
                    "friends_total": None,

                    "subscribers_total": None,
                    "posts_total": None,
                    "videos_total": item.get("videos_count"),
                    "likes_total": (
                        item.get("likes")
                        if item.get("likes") is not None
                        else item.get("like_count")
                    ),
                    "views_total": None,

                    "location": None,
                    "website_url": item.get("bio_link"),
                    "created_date": item.get("create_time"),

                    "is_verified": item.get("is_verified"),
                    "is_private": item.get("is_private"),
                    "is_business": item.get(
                        "is_commerce_user"
                    ),
                    "is_professional": None,

                    "engagement_rate": (
                        item.get("awg_engagement_rate")
                        if item.get("awg_engagement_rate") is not None
                        else item.get("avg_engagement_rate")
                    ),

                    "source_file": source_file,
                })

            # YOUTUBE CHANNEL
            elif (
                "youtube_channel" in source_file_lower
                or "youtube_profile" in source_file_lower
                or "yt_channel" in source_file_lower
            ):
                details = item.get("Details") or {}
                if not isinstance(details, dict):
                    details = {}

                links = item.get("Links") or []

                website_url = None

                if isinstance(links, list) and links:
                    website_url = links[0]
                elif isinstance(links, str):
                    website_url = links

                self.profiles.append({
                    "platform": "youtube",

                    "profile_id": (
                        item.get("identifier")
                        or item.get("id")
                    ),
                    "profile_url": (
                        item.get("url")
                        or item.get("channel_url")
                    ),
                    "profile_handle": (
                        item.get("handle")
                        or self.extract_handle(
                            item.get("url")
                            or item.get("channel_url")
                        )
                    ),
                    "profile_name": (
                        item.get("name")
                        or item.get("channel_name")
                    ),
                    "biography": (
                        item.get("Description")
                        or item.get("description")
                    ),

                    "followers_total": None,
                    "following_total": None,
                    "friends_total": None,

                    "subscribers_total": item.get("subscribers"),
                    "posts_total": None,
                    "videos_total": (
                        item.get("videos_count")
                        if item.get("videos_count") is not None
                        else item.get("total_videos")
                    ),
                    "likes_total": None,
                    "views_total": (
                        item.get("views")
                        if item.get("views") is not None
                        else item.get("total_views")
                    ),

                    "location": details.get("location"),
                    "website_url": website_url,
                    "created_date": item.get("created_date"),

                    "is_verified": item.get("is_verified"),
                    "is_private": None,
                    "is_business": None,
                    "is_professional": None,

                    "engagement_rate": None,

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
    
    def _reset(self, *collections):
        for collection in collections:
            setattr(self, collection, [])
        self.errors = []

    def _record_error(self, file_path, stage, error):
        details = {
            "source_file": file_path.name,
            "stage": stage,
            "error": str(error),
        }
        self.errors.append(details)
        logger.warning(
            "Could not %s %s: %s",
            stage,
            file_path.name,
            error,
        )

    def _extract_files(self, extractors):
        """Parse each bronze file once and pass it to selected extractors."""
        for file_path in sorted(self.bronze_folder.glob("*.json")):
            try:
                data = self.parse_json(file_path)
            except (json.JSONDecodeError, OSError) as error:
                self._record_error(file_path, "parse", error)
                continue

            for extractor in extractors:
                try:
                    extractor(data, file_path.name)
                except (AttributeError, TypeError, ValueError) as error:
                    self._record_error(
                        file_path,
                        extractor.__name__,
                        error,
                    )

    @staticmethod
    def _frame(records, columns):
        return pd.DataFrame.from_records(records).reindex(columns=columns)

    @staticmethod
    def _deduplicate(frame, id_column, keep="last"):
        """De-duplicate real IDs without collapsing rows with missing IDs."""
        if frame.empty:
            return frame

        identifiers = (
            frame[id_column]
            .astype("string")
            .fillna("")
            .str.strip()
        )
        has_id = identifiers.ne("")
        with_id = frame.loc[has_id].drop_duplicates(
            subset=["platform", id_column],
            keep=keep,
        )
        without_id = frame.loc[~has_id]
        return pd.concat([with_id, without_id]).sort_index()

    @staticmethod
    def _to_boolean(series):
        def normalize(value):
            if value is None or value is pd.NA:
                return pd.NA
            if isinstance(value, bool):
                return value
            if isinstance(value, (int, float)) and value in {0, 1}:
                return bool(value)
            if isinstance(value, str):
                lowered = value.strip().lower()
                if lowered in {"true", "1", "yes", "y"}:
                    return True
                if lowered in {"false", "0", "no", "n"}:
                    return False
            return pd.NA

        return series.map(normalize).astype("boolean")

    @staticmethod
    def _convert_numeric(frame, columns):
        for column in columns:
            frame[column] = pd.to_numeric(
                frame[column],
                errors="coerce",
            ).astype("Int64")

    @staticmethod
    def _convert_datetime(frame, column):
        frame[column] = pd.to_datetime(
            frame[column],
            errors="coerce",
            utc=True,
        )

    def _write_frame(self, frame, entity):
        self.silver_folder.mkdir(parents=True, exist_ok=True)
        output_path = (
            self.silver_folder
            / f"{entity}_{self.timestamp}.csv"
        )
        frame.to_csv(output_path, index=False)
        logger.info("Saved %s records to %s", len(frame), output_path)
        return frame

    def _finalize_posts(self):
        frame = self._frame(self.posts, self.POST_COLUMNS)
        frame = self._deduplicate(frame, "post_id")
        self._convert_numeric(
            frame,
            [
                "impressions_total", "impressions_upvotes",
                "impressions_like", "impressions_love",
                "impressions_care", "impressions_wow",
                "impressions_haha", "impressions_angry",
                "impressions_sad", "comments_total", "shares_total",
            ],
        )
        self._convert_datetime(frame, "date_posted")
        frame["is_sponsored"] = self._to_boolean(frame["is_sponsored"])
        return self._write_frame(frame, "posts")

    def _finalize_comments(self):
        frame = self._frame(self.comments, self.COMMENT_COLUMNS)
        frame = self._deduplicate(frame, "comment_id")
        self._convert_numeric(
            frame,
            ["engagement_likes", "replies_total"],
        )
        self._convert_datetime(frame, "date_posted")
        frame["is_reply"] = self._to_boolean(frame["is_reply"])
        return self._write_frame(frame, "comments")

    def _finalize_videos(self):
        frame = self._frame(self.videos, self.VIDEO_COLUMNS)
        frame = self._deduplicate(frame, "video_id")
        self._convert_numeric(
            frame,
            [
                "views_total", "likes_total", "comments_total",
                "shares_total", "saves_total",
            ],
        )
        self._convert_datetime(frame, "date_posted")
        for column in ["is_verified", "is_sponsored"]:
            frame[column] = self._to_boolean(frame[column])
        return self._write_frame(frame, "videos")

    def _finalize_profiles(self):
        frame = self._frame(self.profiles, self.PROFILE_COLUMNS)
        frame = self._deduplicate(frame, "profile_id")
        self._convert_numeric(
            frame,
            [
                "followers_total", "following_total", "friends_total",
                "subscribers_total", "posts_total", "videos_total",
                "likes_total", "views_total",
            ],
        )
        frame["engagement_rate"] = pd.to_numeric(
            frame["engagement_rate"],
            errors="coerce",
        )
        self._convert_datetime(frame, "created_date")
        for column in [
            "is_verified",
            "is_private",
            "is_business",
            "is_professional",
        ]:
            frame[column] = self._to_boolean(frame[column])
        return self._write_frame(frame, "profiles")

    def process_posts(self):
        self._reset("posts")
        self._extract_files([self.extract_posts])
        return self._finalize_posts()

    def process_comments(self):
        self._reset("comments")
        self._extract_files([self.extract_comments])
        return self._finalize_comments()

    def process_videos(self):
        self._reset("videos")
        self._extract_files([self.extract_videos])
        return self._finalize_videos()

    def process_profiles(self):
        self._reset("profiles")
        self._extract_files([self.extract_profiles])
        return self._finalize_profiles()

    def run(self):
        """Parse bronze JSON once, write all silver tables, and return them."""
        self._reset("posts", "comments", "videos", "profiles")
        self._extract_files(
            [
                self.extract_posts,
                self.extract_comments,
                self.extract_videos,
                self.extract_profiles,
            ]
        )

        return {
            "posts": self._finalize_posts(),
            "comments": self._finalize_comments(),
            "videos": self._finalize_videos(),
            "profiles": self._finalize_profiles(),
            "errors": list(self.errors),
        }


if __name__ == "__main__":
    outputs = Transformation().run()
    for entity in ("posts", "comments", "videos", "profiles"):
        print(f"{entity}: {len(outputs[entity])} records")
    print(f"errors: {len(outputs['errors'])}")
