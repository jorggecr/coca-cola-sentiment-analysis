import yt_dlp
import pandas as pd
import langid
from tqdm import tqdm

# Custom logger to silence ffmpeg warnings
class QuietLogger:
    def debug(self, msg): pass
    def warning(self, msg): pass  # Silences warnings
    def error(self, msg): print(msg)

channel_url = "https://www.youtube.com/@Coca-Cola/videos"
print("Starting search for commercials...")

# Get video IDs with over 5M views
flat_opts = {
    'extract_flat': True,
    'quiet': True,
    'skip_download': True,
    'no_warnings': True,
    'logger': QuietLogger()
}

with yt_dlp.YoutubeDL(flat_opts) as ydl:
    flat_info = ydl.extract_info(channel_url, download=False)
    flat_videos = flat_info.get('entries', [])

    filtered_ids = [
        video['id'] for video in flat_videos
        if video.get('view_count') and video['view_count'] > 5_000_000
    ]

print(f"Found {len(filtered_ids)} commercials with over 5 million views.\n")

# Get full video details
full_opts = {
    'extract_flat': False,
    'quiet': True,
    'skip_download': True,
    'no_warnings': True,
    'logger': QuietLogger()
}

final_videos = []

with yt_dlp.YoutubeDL(full_opts) as ydl:
    for idx, vid_id in enumerate(tqdm(filtered_ids, desc="Processing commercials")):
        video_url = f"https://www.youtube.com/watch?v={vid_id}"
        try:
            info = ydl.extract_info(video_url, download=False)
            year = info.get('upload_date', '')[:4] if info.get('upload_date') else "Unknown"
            comment_count = info.get('comment_count') or 0
            has_comments = comment_count > 0

            final_videos.append({
                'video_id': vid_id,
                'title': info.get('title'),
                'views': info.get('view_count'),
                'year': year,
                'link': video_url,
                'comments': has_comments,
                'comment_count': comment_count
            })

        except Exception as e:
            print(f"Error processing {video_url}: {e}")

# Create sorted DataFrame
df = pd.DataFrame(final_videos)
df.sort_values(by=['year', 'views', 'comment_count'], ascending=[True, False, False], inplace=True)

# Detect language
df['language_detected'] = df['title'].astype(str).apply(lambda x: langid.classify(x)[0])

# Save CSV
output_path = "data/raw/commercials.csv"
df.to_csv(output_path, index=False, encoding='utf-8-sig')


print(f"\nCommercial search completed.")

# ----Sumary----

print(f"""
----- Summary -----
File generated: commercials.csv
Commercial saved to: {output_path}
Total commercials found: {len(df)}
Total languages detected: {df['language_detected'].nunique()}
Total comments found: {df['comment_count'].sum()}
Years range: {df['year'].min()} - {df['year'].max()}
""")