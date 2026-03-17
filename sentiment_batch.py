"""
Sentiment batch job: read Silver Parquet, add VADER sentiment, write to silver_enriched.

Runs in a single process (no Spark workers), so no worker-crash issues.
Run after Silver streaming has written data. Gold should read from silver_enriched.

Usage (from project root):
  python sentiment_batch.py

Env:
  SILVER_PARQUET_PATH   - input (default: data/silver)
  SILVER_ENRICHED_PATH  - output (default: data/silver_enriched)
"""
import os

import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

SILVER_PATH = os.getenv("SILVER_PARQUET_PATH", "data/silver")
SILVER_ENRICHED_PATH = os.getenv("SILVER_ENRICHED_PATH", "data/silver_enriched")


def _vader_score(text: str) -> float:
    if pd.isna(text) or not str(text).strip():
        return 0.0
    try:
        return SentimentIntensityAnalyzer().polarity_scores(str(text))["compound"]
    except Exception:
        return 0.0


def run() -> None:
    print(f"Reading Silver from {SILVER_PATH}...")
    df = pd.read_parquet(SILVER_PATH)

    if df.empty:
        print("No rows in Silver. Nothing to enrich.")
        return

    print("Computing VADER sentiment (single process)...")
    df["sentiment_score"] = df["clean_content"].apply(_vader_score)

    os.makedirs(SILVER_ENRICHED_PATH, exist_ok=True)
    # Partition by event_date to match Silver layout for Gold
    partition_cols = ["event_date"] if "event_date" in df.columns else None
    df.to_parquet(SILVER_ENRICHED_PATH, partition_cols=partition_cols, index=False)

    print(f"Wrote {len(df)} rows to {SILVER_ENRICHED_PATH}")
    print("Gold should read from this path (SILVER_PARQUET_PATH=data/silver_enriched).")


if __name__ == "__main__":
    run()
