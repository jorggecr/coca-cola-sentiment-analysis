import csv
import os
import pandas as pd
from urllib.parse import urlparse, parse_qs
from youtube_comment_downloader import YoutubeCommentDownloader
from tqdm import tqdm

# Load commercials with more than 50 comments
df = pd.read_csv("data/raw/commercials.csv")
df = df[df['comment_count'] > 50]

print(f"Starting download of comments...")

print(f"Found {len(df)} commercials with over 50 comments.\n")
# Initialize the comment downloader
downloader = YoutubeCommentDownloader()

# Output path
output_file = "data/raw/comments.csv"
file_exists = os.path.isfile(output_file)

# Open file and write header if needed
with open(output_file, "a", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    if not file_exists:
        writer.writerow(["video_id", "video_title", "author", "text", "likes"])

    # Iterate over videos with progress bar
    for _, row in tqdm(df.iterrows(), total=df.shape[0], desc="Extracting comments"):
        title = row["title"]
        video_url = row["link"]

        # Extract video_id
        parsed_url = urlparse(video_url)
        video_id = parse_qs(parsed_url.query).get("v", [""])[0]
        if not video_id:
            continue  # Skip invalid links

        try:
            comments = downloader.get_comments_from_url(video_url)

            # Write up to 100 comments per video
            for i, comment in enumerate(comments):
                if i >= 100:
                    break
                writer.writerow([
                    video_id,
                    title,
                    comment.get("author", "").strip(),
                    comment.get("text", "").strip(),
                    comment.get("votes", 0)
                ])
        except Exception:
            continue  # Silently skip errors to keep it minimal4

print(f"\nComments download completed.")

df = pd.read_csv('data/raw/comments.csv')
# ----Sumary----
print(f"""
---- Summary ----
File generated: comments.csv
Comments saved to: {output_file}
Total comments downloaded: {df['text'].notna().sum()}
Total number of authors commenting: {df['author'].nunique()}
Total final commercials: {df['video_id'].nunique()}
""")