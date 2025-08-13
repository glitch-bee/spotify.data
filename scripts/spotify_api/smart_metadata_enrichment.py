"""
Spotify API metadata enrichment with streaming writes and on-disk joins.

Dependencies:
    pip install duckdb psutil
    (sqlite3 is part of the Python standard library)
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
from pathlib import Path
from typing import Dict, Iterable, Set, Tuple

import duckdb
import pandas as pd
import spotipy
from dotenv import load_dotenv
from spotipy.oauth2 import SpotifyOAuth
from tqdm import tqdm

try:  # Optional dependency for memory metrics
    import psutil  # type: ignore
except Exception:  # pragma: no cover - psutil is optional
    psutil = None


load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration & paths
# ---------------------------------------------------------------------------

CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")

if not CLIENT_ID or not CLIENT_SECRET:
    raise ValueError("Please set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET in your .env file")

SCRIPT_DIR = Path(__file__).resolve().parent
BASE_DIR = (SCRIPT_DIR / ".." / "..").resolve()

PROCESSED_DIR = BASE_DIR / "data" / "processed"
ENRICHED_DIR = BASE_DIR / "data" / "enriched"

BASE_CSV = PROCESSED_DIR / "cleaned_streaming_history.csv"
META_CSV = ENRICHED_DIR / "spotify_api_metadata.csv"
FINAL_CSV = ENRICHED_DIR / "spotify_api_enriched_streaming_history.csv"
PROGRESS_DB = ENRICHED_DIR / "api_progress.sqlite"

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
DELAY_BETWEEN_REQUESTS = float(os.getenv("DELAY_BETWEEN_REQUESTS", "0.3"))
DELAY_BETWEEN_BATCHES = float(os.getenv("DELAY_BETWEEN_BATCHES", "5"))
TQDM_DISABLE = os.getenv("TQDM_DISABLE") == "1"


# ---------------------------------------------------------------------------
# Spotipy client
# ---------------------------------------------------------------------------

scope = "user-library-read"
auth_manager = SpotifyOAuth(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    redirect_uri="http://127.0.0.1:8888/callback",
    scope=scope,
    cache_path=".spotify_cache",
)
sp = spotipy.Spotify(auth_manager=auth_manager)


# ---------------------------------------------------------------------------
# SQLite progress store
# ---------------------------------------------------------------------------

def get_db_connection() -> sqlite3.Connection:
    """Open the progress database creating the table if needed."""
    ENRICHED_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(PROGRESS_DB)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS progress(
            key TEXT PRIMARY KEY,
            meta_json TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    return conn


def load_processed_keys(conn: sqlite3.Connection) -> Set[str]:
    """Return the set of track keys already processed."""
    try:
        rows = conn.execute("SELECT key FROM progress").fetchall()
        return {r[0] for r in rows}
    except sqlite3.Error:
        return set()


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def load_existing_enrichment() -> Set[str]:
    """Tracks that already have external metadata (Ultimate Spotify)."""
    enriched_tracks: Set[str] = set()
    ultimate_file = ENRICHED_DIR / "ultimate_spotify_enriched_streaming_history.csv"
    if not ultimate_file.exists():
        return enriched_tracks

    try:
        df = pd.read_csv(ultimate_file)
        metadata_cols = ["acousticness", "danceability", "energy", "valence", "genre"]
        has_metadata = df[metadata_cols].notna().any(axis=1)
        for row in df.loc[has_metadata, [
            "master_metadata_track_name",
            "master_metadata_album_artist_name",
        ]].itertuples(index=False):
            enriched_tracks.add(f"{row[0]}|||{row[1]}")
        logger.info("Found %d tracks already enriched with external metadata", len(enriched_tracks))
    except Exception as exc:  # pragma: no cover - best effort only
        logger.warning("Failed loading existing enrichment: %s", exc)
    return enriched_tracks


def search_track_metadata(track_name: str, artist_name: str) -> Dict[str, object] | None:
    """Search Spotify for track metadata with basic retry/backoff."""
    query = f"track:{track_name} artist:{artist_name}"
    for attempt in range(3):
        try:
            results = sp.search(q=query, type="track", limit=1)
            items = results["tracks"]["items"]
            if not items:
                return None
            track_data = items[0]
            track_id = track_data.get("id")

            artist_data = None
            try:
                artist_id = track_data["artists"][0]["id"]
                artist_data = sp.artist(artist_id)
            except Exception as err:  # pragma: no cover - best effort
                logger.warning("Failed to get artist data for %s: %s", artist_name, err)

            album_data = track_data.get("album", {})

            return {
                "spotify_id": track_id,
                "spotify_uri": track_data.get("uri"),
                "track_popularity": track_data.get("popularity"),
                "track_duration_ms": track_data.get("duration_ms"),
                "track_explicit": track_data.get("explicit"),
                "track_preview_url": track_data.get("preview_url"),
                "album_name": album_data.get("name"),
                "album_release_date": album_data.get("release_date"),
                "album_release_date_precision": album_data.get("release_date_precision"),
                "album_total_tracks": album_data.get("total_tracks"),
                "album_type": album_data.get("album_type"),
                "artist_popularity": artist_data.get("popularity") if artist_data else None,
                "artist_followers": artist_data.get("followers", {}).get("total") if artist_data else None,
                "api_genres": ", ".join(artist_data.get("genres", [])) if artist_data else None,
                "audio_features_note": "Skipped due to Spotify API limitations",
                "enrichment_source": "spotify_api",
            }
        except Exception as exc:
            wait = 2 ** attempt
            logger.warning(
                "Error fetching %s by %s: %s. Retrying in %s sec",
                track_name,
                artist_name,
                exc,
                wait,
            )
            time.sleep(wait)
    return None


def process_batch(
    batch_df: pd.DataFrame,
    processed_keys: Set[str],
    conn: sqlite3.Connection,
    meta_path: Path,
) -> Tuple[int, int, int, int]:
    """Process a batch of tracks and append results to CSV."""

    rows = []
    api_hits = 0
    upserts = 0
    skipped = 0

    for track, artist in batch_df[[
        "master_metadata_track_name",
        "master_metadata_album_artist_name",
    ]].itertuples(index=False):
        key = f"{track}|||{artist}"
        if key in processed_keys:
            skipped += 1
            continue

        metadata = search_track_metadata(track, artist)
        api_hits += 1

        row = {
            "master_metadata_track_name": track,
            "master_metadata_album_artist_name": artist,
        }
        if metadata:
            row.update(metadata)
        else:
            row.update({
                "spotify_id": None,
                "spotify_uri": None,
                "track_popularity": None,
                "track_duration_ms": None,
                "track_explicit": None,
                "track_preview_url": None,
                "album_name": None,
                "album_release_date": None,
                "album_release_date_precision": None,
                "album_total_tracks": None,
                "album_type": None,
                "artist_popularity": None,
                "artist_followers": None,
                "api_genres": None,
                "audio_features_note": None,
                "enrichment_source": "spotify_api",
            })
        rows.append(row)

        conn.execute(
            "INSERT OR REPLACE INTO progress(key, meta_json) VALUES(?, ?)",
            (key, json.dumps(metadata)),
        )
        processed_keys.add(key)
        upserts += 1

    processed = len(rows)
    if rows:
        header = not meta_path.exists()
        pd.DataFrame(rows).to_csv(meta_path, index=False, mode="a", header=header)

    return processed, skipped, api_hits, upserts


# ---------------------------------------------------------------------------
# DuckDB merge
# ---------------------------------------------------------------------------

def run_duckdb_merge(
    base_csv: Path | str = BASE_CSV,
    meta_csv: Path | str = META_CSV,
    out_csv: Path | str = FINAL_CSV,
) -> None:
    """Perform on-disk merge of streaming history and API metadata."""

    base_csv = Path(base_csv)
    meta_csv = Path(meta_csv)
    out_csv = Path(out_csv)

    if not base_csv.exists() or not meta_csv.exists():
        logger.warning("Missing inputs for DuckDB merge: base=%s meta=%s", base_csv, meta_csv)
        return

    con = duckdb.connect(database=":memory:")

    con.execute(
        f"""
        CREATE TABLE base AS
        SELECT *,
               lower(trim(master_metadata_track_name)) AS track_key,
               lower(trim(master_metadata_album_artist_name)) AS artist_key
        FROM read_csv_auto('{base_csv}')
        """
    )

    con.execute(
        f"""
        CREATE TABLE meta_raw AS
        SELECT *,
               lower(trim(master_metadata_track_name)) AS track_key,
               lower(trim(master_metadata_album_artist_name)) AS artist_key
        FROM read_csv_auto('{meta_csv}')
        """
    )

    dupes = con.execute(
        """
        SELECT track_key, artist_key, COUNT(*) c
        FROM meta_raw
        GROUP BY 1,2
        HAVING c > 1
        """
    ).fetchall()
    if dupes:
        logger.warning("Dropping %d duplicate metadata keys", len(dupes))
        con.execute(
            """
            CREATE TABLE meta AS
            SELECT * EXCLUDE(rn) FROM (
                SELECT *,
                       row_number() OVER (PARTITION BY track_key, artist_key ORDER BY track_key) rn
                FROM meta_raw
            )
            WHERE rn = 1
            """
        )
    else:
        con.execute("CREATE TABLE meta AS SELECT * FROM meta_raw")

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"""
        COPY (
            SELECT b.*, m.* EXCLUDE(track_key, artist_key)
            FROM base b
            LEFT JOIN meta m USING(track_key, artist_key)
        ) TO '{out_csv}' (HEADER, DELIMITER ',')
        """
    )

    base_rows = con.execute("SELECT COUNT(*) FROM base").fetchone()[0]
    logger.info("DuckDB merge wrote %s rows to %s", base_rows, out_csv)
    con.close()


# ---------------------------------------------------------------------------
# Main enrichment flow
# ---------------------------------------------------------------------------

def main(merge_only: bool = False) -> None:
    if merge_only:
        run_duckdb_merge()
        return

    try:
        user = sp.current_user()
        logger.info("Authenticated as %s (%s)", user.get("display_name"), user.get("id"))
    except Exception as exc:
        logger.error("Authentication failed: %s", exc)
        return

    if not BASE_CSV.exists():
        logger.error("Base streaming history not found: %s", BASE_CSV)
        return

    conn = get_db_connection()
    processed_keys = load_processed_keys(conn)
    logger.info("Loaded %d processed keys from progress DB", len(processed_keys))

    df = pd.read_csv(BASE_CSV)
    logger.info("Loaded %d total streaming records", len(df))

    unique_tracks = (
        df[["master_metadata_track_name", "master_metadata_album_artist_name"]]
        .dropna()
        .drop_duplicates()
        .reset_index(drop=True)
    )
    logger.info("Found %d unique tracks", len(unique_tracks))

    already_enriched = load_existing_enrichment()
    if already_enriched:
        unique_tracks["key"] = (
            unique_tracks["master_metadata_track_name"]
            + "|||"
            + unique_tracks["master_metadata_album_artist_name"]
        )
        unique_tracks = unique_tracks[~unique_tracks["key"].isin(already_enriched)].drop(columns=["key"]).reset_index(drop=True)
    logger.info("After filtering existing enrichment: %d tracks", len(unique_tracks))

    if unique_tracks.empty:
        logger.info("All tracks already have metadata; running merge only")
        conn.close()
        run_duckdb_merge()
        return

    total_batches = (len(unique_tracks) + BATCH_SIZE - 1) // BATCH_SIZE

    for start in tqdm(
        range(0, len(unique_tracks), BATCH_SIZE),
        disable=TQDM_DISABLE,
        desc="Processing batches",
    ):
        batch_num = start // BATCH_SIZE + 1
        batch_df = unique_tracks.iloc[start : start + BATCH_SIZE]
        processed, skipped, api_hits, upserts = process_batch(batch_df, processed_keys, conn, META_CSV)
        conn.commit()
        logger.info(
            "batch=%s processed=%s skipped=%s api_hits=%s sqlite_upserts=%s",
            batch_num,
            processed,
            skipped,
            api_hits,
            upserts,
        )

        if batch_num % 10 == 0 and psutil:
            try:  # pragma: no cover - best effort only
                rss = psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)
                logger.info("memory_rss_mb=%.1f", rss)
            except Exception:
                pass

        if start + BATCH_SIZE < len(unique_tracks):
            time.sleep(DELAY_BETWEEN_BATCHES)

    conn.close()

    run_duckdb_merge()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Spotify API metadata enrichment")
    parser.add_argument(
        "--merge-only",
        action="store_true",
        help="run only the DuckDB merge step",
    )
    args = parser.parse_args()
    main(merge_only=args.merge_only)

