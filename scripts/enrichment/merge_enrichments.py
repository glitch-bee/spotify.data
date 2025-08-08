#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

BASE = ROOT / "data/processed/cleaned_streaming_history.csv"
KAGGLE = ROOT / "data/enriched/ultimate_spotify_enriched_streaming_history.csv"
API = ROOT / "data/enriched/spotify_api_enriched_streaming_history.csv"
OUT = ROOT / "data/enriched/final_enriched_streaming_history.csv"

# Columns to keep from API and Kaggle to avoid collisions
API_KEEP = {
    "spotify_id", "spotify_uri", "track_popularity", "track_duration_ms", "track_explicit",
    "track_preview_url", "album_name", "album_release_date", "album_release_date_precision",
    "album_total_tracks", "album_type", "artist_popularity", "artist_followers", "api_genres"
}

KAGGLE_KEEP = {
    "track_id", "popularity", "acousticness", "danceability", "duration_ms", "energy",
    "instrumentalness", "key", "liveness", "loudness", "mode", "speechiness", "tempo",
    "time_signature", "valence", "genre"
}

JOIN_KEYS = ["master_metadata_track_name", "master_metadata_album_artist_name"]


def main():
    base = pd.read_csv(BASE)
    # Start with base
    df = base.copy()

    # Join Kaggle first
    if KAGGLE.exists():
        k = pd.read_csv(KAGGLE)
        k_cols = [c for c in k.columns if c in KAGGLE_KEEP or c in JOIN_KEYS]
        df = df.merge(k[k_cols], on=JOIN_KEYS, how="left", suffixes=("", "_k"))

    # Join API next
    if API.exists():
        a = pd.read_csv(API)
        a_cols = [c for c in a.columns if c in API_KEEP or c in JOIN_KEYS]
        df = df.merge(a[a_cols], on=JOIN_KEYS, how="left", suffixes=("", "_api"))

    # Write out
    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT, index=False)
    print(f"Saved final enriched dataset to: {OUT}")


if __name__ == "__main__":
    main()
