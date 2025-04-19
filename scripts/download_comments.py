import csv
import os
import pandas as pd
from urllib.parse import urlparse, parse_qs
from youtube_comment_downloader import YoutubeCommentDownloader
from tqdm import tqdm

# Cargar comerciales con más de 50 comentarios
df = pd.read_csv("data/raw/commercials.csv")
df = df[df['comment_count'] > 50]

print(f"Starting download of comments...")
print(f"Found {len(df)} commercials with over 50 comments.\n")

# Inicializar descargador
downloader = YoutubeCommentDownloader()

# Ruta de salida
output_file = "data/raw/comments.csv"

# Eliminar el archivo si ya existe
if os.path.isfile(output_file):
    os.remove(output_file)

# Set para evitar comentarios duplicados
seen = set()

# Abrir archivo y escribir encabezado
with open(output_file, "w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(["video_id", "video_title", "link", "author", "text", "likes"])

    for _, row in tqdm(df.iterrows(), total=df.shape[0], desc="Extracting comments"):
        title = row["video_title"]
        video_url = row["link"]

        # Extraer video_id
        parsed_url = urlparse(video_url)
        video_id = parse_qs(parsed_url.query).get("v", [""])[0]
        if not video_id:
            continue

        try:
            comments = downloader.get_comments_from_url(video_url)

            count = 0
            for comment in comments:
                if count >= 100:
                    break

                author = comment.get("author", "").strip()
                text = comment.get("text", "").strip()
                key = (video_id, author, text)

                if not text or key in seen:
                    continue

                seen.add(key)
                writer.writerow([
                    video_id,
                    title,
                    video_url,
                    author,
                    text,
                    comment.get("votes", 0)
                ])
                count += 1

        except Exception as e:
            print(f"Error in {video_url}: {e}")
            continue

print(f"\nComments download completed.")

# Resumen
df = pd.read_csv(output_file)
print(f"""
---- Summary ----
File generated: comments.csv
Comments saved to: {output_file}
Total comments downloaded: {df['text'].notna().sum()}
Total number of authors commenting: {df['author'].nunique()}
Total final commercials: {df['video_id'].nunique()}
""")
