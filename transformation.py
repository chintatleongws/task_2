import json
from pathlib import Path
import pandas as pd

class Transformation:
    def __init__(self, base_folder_path="./data/bronze/raw_json"):
        self.folder = Path(base_folder_path)
        self.posts = []

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

    def run(self):
        for file in self.folder.glob("*.json"):
            data = self.parse_json(file)
            self.extract_posts(data, file.name)

        posts_df = pd.DataFrame(self.posts)
        posts_df = posts_df.drop_duplicates(subset=["platform", "post_id"])

        posts_df.to_csv("posts.csv", index=False)
        print("Saved posts.csv")

        return posts_df


transformer = Transformation()
posts_df = transformer.run()

print(posts_df.head().to_string(index=False))