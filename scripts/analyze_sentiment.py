import pandas as pd
import torch
import torch.nn.functional as F
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from tqdm import tqdm
import os 

# Load model and tokenizer
model_name = "cardiffnlp/twitter-roberta-base-sentiment"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForSequenceClassification.from_pretrained(model_name)

# Use GPU if available
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model.to(device)
model.eval()

print(f"Starting sentiment analysis...")
print(f"Using device: {device}")

# Load comments and drop empty ones
df = pd.read_csv("data/raw/comments.csv")
df.dropna(subset=["text"], inplace=True)

print(f"Analyzing {len(df)} total comments.\n")

# Batch sentiment classification
def classify_batch(texts, batch_size=32):
    sentiments = []
    confidences = []

    for i in tqdm(range(0, len(texts), batch_size), desc="Analyzing sentiment"):
        batch_texts = texts[i:i + batch_size]
        encodings = tokenizer(batch_texts, return_tensors="pt", padding=True, truncation=True, max_length=512).to(device)

        with torch.no_grad():
            outputs = model(**encodings)
            probs = F.softmax(outputs.logits, dim=1)
            batch_confidences, batch_predictions = torch.max(probs, dim=1)

        labels = {0: "negative", 1: "neutral", 2: "positive"}
        sentiments += [labels.get(pred.item(), "unknown") for pred in batch_predictions.cpu()]
        confidences += batch_confidences.cpu().tolist()

    return sentiments, confidences

# Apply classification
sentiments, confidences = classify_batch(df["text"].tolist())
df["sentiment"] = sentiments
df["confidence_score"] = confidences

# Save results
output_path = "data/processed/sentiment.csv"

# Delete file if it exists
if os.path.isfile(output_path):
    os.remove(output_path)

df.to_csv(output_path, index=False)

print(f"\nSentiment analysis completed.")

df = pd.read_csv(output_path)

# ----Summary----
print(f"""
----- Summary -----
File generated: sentiment.csv
Sentiment saved to: {output_path}
Total comments analyzed: {len(df)}
Average confidence score: {df['confidence_score'].mean():.2f}
Sentiment distribution:
  Negative: {sum(df['sentiment'] == 'negative')}
  Neutral:  {sum(df['sentiment'] == 'neutral')}
  Positive: {sum(df['sentiment'] == 'positive')}
Author with the most comments: {df['author'].value_counts().idxmax()}
""")
