"""
Smart Spotify API Metadata Enrichment (Memory-Safe)

Deps (install before running):
    pip install spotipy python-dotenv tqdm duckdb psutil
    # SQLite uses stdlib 'sqlite3' (no extra install required)

Key design:
  - Stream results to CSV per batch; no global accumulation.
  - Crash-safe progress in SQLite: data/enriched/api_progress.sqlite
      table progress(key TEXT PRIMARY KEY, meta_json TEXT,
                     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
  - Skip tracks enriched via external dataset and already API-processed (SQLite).
  - Final LEFT JOIN done on disk in DuckDB; no Pandas full-frame merge.
  - Normalize join keys: lower(trim()) on both sides; enforce uniqueness for meta.
  - Avoid printing large DataFrames; log concise shapes and counters only.
"""

from __future__ import annotations

import os
import gc
import json
import time
import sqlite3
import logging
import resource
from pathlib import Path
from typing import Dict, List, Optional, Set

try:  # optional memory stats
    import psutil
except Exception:  # pragma: no cover
    psutil = None

import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from tqdm import tqdm
from dotenv import load_dotenv

from .duckdb_merge import run_duckdb_merge


# ---------------------------------------------------------------------------
# Config & Logging
# ---------------------------------------------------------------------------

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# Credentials
CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
if not CLIENT_ID or not CLIENT_SECRET:
    raise ValueError("Please set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET in your .env file")


# Paths
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
PROCESSED_DIR = DATA_DIR / "processed"
ENRICHED_DIR = DATA_DIR / "enriched"

BASE_CSV = PROCESSED_DIR / "cleaned_streaming_history.csv"
META_CSV = ENRICHED_DIR / "spotify_api_metadata.csv"
FINAL_CSV = ENRICHED_DIR / "spotify_api_enriched_streaming_history.csv"
PROGRESS_DB = ENRICHED_DIR / "api_progress.sqlite"

# External enrichment skip file
ULTIMATE_ENRICHED_CSV = ENRICHED_DIR / "ultimate_spotify_enriched_streaming_history.csv"


# Processing config
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
DELAY_BETWEEN_REQUESTS = 0.3
DELAY_BETWEEN_BATCHES = 5
SUMMARY_EVERY_N_BATCHES = 10


# Spotify auth
SCOPE = "user-library-read"
REDIRECT_URI = "http://127.0.0.1:8888/callback"


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def ensure_dirs() -> None:
    ENRICHED_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def mem_mb() -> float:
    if psutil is not None:
        try:
            return psutil.Process().memory_info().rss / (1024 * 1024)
        except Exception:
            pass
    rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return rss_kb / 1024.0


def norm(s: Optional[str]) -> str:
    if s is None:
        return ""
    return str(s).strip().lower()


def make_key(track: str, artist: str) -> str:
    return f"{norm(track)}|||{norm(artist)}"


def read_csv_robust(path: Path, **kwargs):
    """Read CSV with utf-8 and encoding error tolerance; supports chunksize iterator."""
    try:
        return pd.read_csv(path, encoding="utf-8", encoding_errors="replace", **kwargs)
    except TypeError:  # older pandas
        return pd.read_csv(path, encoding="utf-8", **kwargs)


# ---------------------------------------------------------------------------
# SQLite progress
# ---------------------------------------------------------------------------

def init_progress_db() -> None:
    ensure_dirs()
    with sqlite3.connect(PROGRESS_DB) as con:
        # Ensure table exists with current schema
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS progress (
                key TEXT PRIMARY KEY,
                meta_json TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        # Migrate older schemas by adding missing columns
        cols = {row[1] for row in con.execute("PRAGMA table_info(progress)").fetchall()}
        if "meta_json" not in cols:
            con.execute("ALTER TABLE progress ADD COLUMN meta_json TEXT")
        if "updated_at" not in cols:
            con.execute(
                "ALTER TABLE progress ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            )


def load_completed_keys() -> Set[str]:
    init_progress_db()
    with sqlite3.connect(PROGRESS_DB) as con:
        rows = con.execute("SELECT key FROM progress WHERE meta_json IS NOT NULL").fetchall()
    return {r[0] for r in rows}


def upsert_progress(batch_key_to_meta: Dict[str, Dict]) -> int:
    if not batch_key_to_meta:
        return 0
    init_progress_db()
    inserted = 0
    with sqlite3.connect(PROGRESS_DB) as con:
        cur = con.cursor()
        for k, meta in batch_key_to_meta.items():
            cur.execute(
                "INSERT OR REPLACE INTO progress(key, meta_json, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
                (k, json.dumps(meta, ensure_ascii=False)),
            )
            inserted += 1
        con.commit()
    return inserted


# ---------------------------------------------------------------------------
# External enrichment skip list
# ---------------------------------------------------------------------------

def load_existing_enrichment() -> Set[str]:
    """Return keys that are already enriched by the external (Kaggle) dataset.

    IMPORTANT: Only consider rows that look actually enriched (e.g., have a Kaggle
    track_id or popularity). The external CSV mirrors the full play history; if we
    don't filter, we'd skip almost everything erroneously.
    """
    enriched: Set[str] = set()
    if not ULTIMATE_ENRICHED_CSV.exists():
        return enriched
    logger.info("Loading existing enrichment keys (external dataset) with Kaggle-match signal…")

    # Prefer 'track_id' as a strong Kaggle match signal; fallback to 'popularity'.
    wanted_cols = [
        "master_metadata_track_name",
        "master_metadata_album_artist_name",
        "track_id",
        "popularity",
    ]

    # Read in chunks to avoid memory spikes
    for chunk in read_csv_robust(
        ULTIMATE_ENRICHED_CSV,
        usecols=lambda c: c in wanted_cols,  # guard if some columns are absent
        chunksize=100_000,
    ):
        # Ensure baseline columns exist
        if not {"master_metadata_track_name", "master_metadata_album_artist_name"}.issubset(chunk.columns):
            continue

        # Determine enriched rows: track_id present (preferred), else popularity present
        has_track_id = "track_id" in chunk.columns
        has_popularity = "popularity" in chunk.columns

        if has_track_id:
            mask = chunk["track_id"].notna() & (chunk["track_id"].astype(str).str.strip() != "")
        elif has_popularity:
            mask = chunk["popularity"].notna()
        else:
            # If no signal columns are present in this chunk, skip it (be conservative)
            continue

        sub = chunk.loc[mask, ["master_metadata_track_name", "master_metadata_album_artist_name"]]
        sub = sub.dropna()
        for t, a in zip(sub["master_metadata_track_name"], sub["master_metadata_album_artist_name"]):
            enriched.add(make_key(t, a))

    logger.info(f"Loaded {len(enriched)} external-enriched unique keys (Kaggle-matched)")
    return enriched


# ---------------------------------------------------------------------------
# Spotify API helpers
# ---------------------------------------------------------------------------

def spotify_client() -> spotipy.Spotify:
    auth_manager = SpotifyOAuth(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        redirect_uri=REDIRECT_URI,
        scope=SCOPE,
        cache_path=str(BASE_DIR / ".spotify_cache"),
    )
    return spotipy.Spotify(auth_manager=auth_manager)


def search_track_metadata(sp: spotipy.Spotify, track_name: str, artist_name: str) -> Optional[Dict]:
    query = f"track:{track_name} artist:{artist_name}"
    for attempt in range(3):
        try:
            results = sp.search(q=query, type="track", limit=1)
            items = results.get("tracks", {}).get("items", [])
            if not items:
                return None
            tr = items[0]
            track_id = tr.get("id")

            artist_meta = None
            try:
                aid = tr.get("artists", [{}])[0].get("id")
                if aid:
                    artist_meta = sp.artist(aid)
            except Exception as e:
                logger.warning(f"Artist fetch failed for {artist_name}: {e}")

            album = tr.get("album", {})

            return {
                "spotify_id": track_id,
                "spotify_uri": tr.get("uri"),
                "track_popularity": tr.get("popularity"),
                "track_duration_ms": tr.get("duration_ms"),
                "track_explicit": tr.get("explicit"),
                "track_preview_url": tr.get("preview_url"),
                "album_name": album.get("name"),
                "album_release_date": album.get("release_date"),
                "album_release_date_precision": album.get("release_date_precision"),
                "album_total_tracks": album.get("total_tracks"),
                "album_type": album.get("album_type"),
                "artist_popularity": (artist_meta or {}).get("popularity"),
                "artist_followers": (artist_meta or {}).get("followers", {}).get("total"),
                "api_genres": ", ".join((artist_meta or {}).get("genres", []) or []),
                "audio_features_note": "Skipped due to Spotify API limitations",
                "enrichment_source": "spotify_api",
            }
        except Exception as e:
            wait = 2 ** attempt
            logger.warning(
                f"Search error for {track_name} by {artist_name}: {e}; retrying in {wait}s"
            )
            time.sleep(wait)
    return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(merge_only: bool = False) -> None:
    ensure_dirs()

    if merge_only:
        if not BASE_CSV.exists() or not META_CSV.exists():
            logger.warning("Required CSVs missing; merge skipped.")
            return
        rows_written, meta_rows = run_duckdb_merge(BASE_CSV, META_CSV, FINAL_CSV)
        logger.info(
            f"DuckDB merge complete: final_rows={rows_written}, meta_rows={meta_rows}, output='{FINAL_CSV}'"
        )
        return

    logger.info("Starting smart Spotify API metadata enrichment (memory-safe)")

    sp = spotify_client()
    try:
        user = sp.current_user()
        logger.info(f"Authenticated as: {user.get('display_name')} ({user.get('id')})")
    except Exception as e:
        logger.error(f"Authentication failed: {e}")
        return

    if not BASE_CSV.exists():
        logger.error(f"Base CSV not found: {BASE_CSV}")
        return

    cols = ["master_metadata_track_name", "master_metadata_album_artist_name"]
    base_df = read_csv_robust(
        BASE_CSV,
        usecols=cols,
        dtype="string",
    ).dropna(subset=cols).drop_duplicates().reset_index(drop=True)
    logger.info(f"Base unique tracks: shape={base_df.shape}, mem≈{mem_mb():.1f}MB")

    base_df["_tkey"] = base_df["master_metadata_track_name"].str.strip().str.lower()
    base_df["_akey"] = base_df["master_metadata_album_artist_name"].str.strip().str.lower()
    base_df["_key"] = base_df["_tkey"] + "|||" + base_df["_akey"]

    external_keys = load_existing_enrichment()
    completed_keys = load_completed_keys()

    base_df = base_df[
        ~base_df["_key"].isin(external_keys)
        & ~base_df["_key"].isin(completed_keys)
    ].reset_index(drop=True)
    logger.info(
        f"After skip filters: to_process={len(base_df)} (skipped external={len(external_keys)}, already_api={len(completed_keys)})"
    )

    if len(base_df) == 0:
        logger.info("Nothing to process. Proceeding to DuckDB merge with existing metadata CSV (if any)...")

    header_exists = META_CSV.exists()
    if not header_exists:
        META_CSV.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            columns=[
                "master_metadata_track_name",
                "master_metadata_album_artist_name",
                "spotify_id",
                "spotify_uri",
                "track_popularity",
                "track_duration_ms",
                "track_explicit",
                "track_preview_url",
                "album_name",
                "album_release_date",
                "album_release_date_precision",
                "album_total_tracks",
                "album_type",
                "artist_popularity",
                "artist_followers",
                "api_genres",
                "audio_features_note",
                "enrichment_source",
                "key",
            ]
        ).to_csv(META_CSV, index=False)

    total_success = 0
    total_attempted = 0

    for start in tqdm(
        range(0, len(base_df), BATCH_SIZE),
        desc="Batches",
        disable=os.getenv("TQDM_DISABLE") == "1",
    ):
        end = min(start + BATCH_SIZE, len(base_df))
        batch = base_df.iloc[start:end].copy()
        logger.info(f"Batch {start//BATCH_SIZE + 1}: size={len(batch)} mem≈{mem_mb():.1f}MB")

        batch_results: List[Dict] = []
        batch_completed: Dict[str, Dict] = {}

        try:
            for _, row in batch.iterrows():
                t = row["master_metadata_track_name"]
                a = row["master_metadata_album_artist_name"]
                k = row["_key"]

                meta = search_track_metadata(sp, t, a)
                total_attempted += 1
                if meta is not None:
                    total_success += 1
                    batch_completed[k] = meta
                    rec = {
                        "master_metadata_track_name": t,
                        "master_metadata_album_artist_name": a,
                        **meta,
                        "key": k,
                    }
                    batch_results.append(rec)

                time.sleep(DELAY_BETWEEN_REQUESTS)
        finally:
            processed = len(batch_results)
            skipped = len(batch) - processed
            if batch_results:
                df_batch = pd.DataFrame(batch_results)
                with open(META_CSV, "a", encoding="utf-8", errors="replace") as fh:
                    df_batch.to_csv(fh, header=False, index=False)
                inserted = upsert_progress(batch_completed)
                logger.info(
                    f"batch_stats processed={processed} skipped={skipped} api_hits={len(batch)} sqlite_upserts={inserted} total_attempted={total_attempted} total_success={total_success}"
                )
                del df_batch

            del batch_results, batch_completed, batch
            gc.collect()

        if end < len(base_df):
            logger.info(
                f"Sleeping {DELAY_BETWEEN_BATCHES}s before next batch… mem≈{mem_mb():.1f}MB"
            )
            time.sleep(DELAY_BETWEEN_BATCHES)

        if (start // BATCH_SIZE + 1) % SUMMARY_EVERY_N_BATCHES == 0:
            logger.info(
                f"Summary: batches_done={start//BATCH_SIZE + 1} total_attempted={total_attempted} total_success={total_success} mem≈{mem_mb():.1f}MB"
            )

    if META_CSV.exists():
        logger.info("Running DuckDB merge (left join) to produce final enriched CSV…")
        rows_written, meta_rows = run_duckdb_merge(BASE_CSV, META_CSV, FINAL_CSV)
        logger.info(
            f"DuckDB merge complete: final_rows={rows_written}, meta_rows={meta_rows}, output='{FINAL_CSV}'"
        )
    else:
        logger.info("No metadata CSV found; skipping DuckDB merge.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Smart Spotify API metadata enrichment"
    )
    parser.add_argument(
        "--merge-only", action="store_true", help="Run only the DuckDB join"
    )
    args = parser.parse_args()
    main(merge_only=args.merge_only)

