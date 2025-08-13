"""Split merged streaming history into songs-only and podcasts-only CSVs using DuckDB.

Input (default): scripts/spotify_api/data/enriched/spotify_api_enriched_streaming_history.csv
Outputs:
  - scripts/spotify_api/data/enriched/spotify_api_enriched_streaming_history_songs.csv
  - scripts/spotify_api/data/enriched/spotify_api_enriched_streaming_history_podcasts.csv
"""

from __future__ import annotations

import sys
from pathlib import Path
import duckdb


BASE_DIR = Path(__file__).resolve().parent
ENRICHED_DIR = BASE_DIR / "data" / "enriched"
DEFAULT_IN = ENRICHED_DIR / "spotify_api_enriched_streaming_history.csv"
SONGS_OUT = ENRICHED_DIR / "spotify_api_enriched_streaming_history_songs.csv"
POD_OUT = ENRICHED_DIR / "spotify_api_enriched_streaming_history_podcasts.csv"


def split_media_types(in_csv: Path = DEFAULT_IN) -> tuple[int, int]:
    ENRICHED_DIR.mkdir(parents=True, exist_ok=True)
    if not in_csv.exists():
        raise FileNotFoundError(f"Input not found: {in_csv}")

    if SONGS_OUT.exists():
        SONGS_OUT.unlink()
    if POD_OUT.exists():
        POD_OUT.unlink()

    con = duckdb.connect()
    in_posix = in_csv.as_posix()
    songs_sql = f"""
    WITH base AS (
        SELECT *
        FROM read_csv_auto(
            '{in_posix}',
            ALL_VARCHAR=TRUE,
            sample_size=-1,
            strict_mode=false,
            ignore_errors=true,
            parallel=false,
            allow_quoted_nulls=true
        )
    ), tagged AS (
        SELECT *,
            CASE
              WHEN coalesce(spotify_episode_uri, '') <> ''
                   OR coalesce(episode_name, '') <> ''
                   OR coalesce(episode_show_name, '') <> ''
              THEN 'podcast' ELSE 'song'
            END AS media_type
        FROM base
    )
    SELECT * FROM tagged WHERE media_type='song'
    """

    pods_sql = f"""
    WITH base AS (
        SELECT *
        FROM read_csv_auto(
            '{in_posix}',
            ALL_VARCHAR=TRUE,
            sample_size=-1,
            strict_mode=false,
            ignore_errors=true,
            parallel=false,
            allow_quoted_nulls=true
        )
    ), tagged AS (
        SELECT *,
            CASE
              WHEN coalesce(spotify_episode_uri, '') <> ''
                   OR coalesce(episode_name, '') <> ''
                   OR coalesce(episode_show_name, '') <> ''
              THEN 'podcast' ELSE 'song'
            END AS media_type
        FROM base
    )
    SELECT * FROM tagged WHERE media_type='podcast'
    """

    # Write songs-only
    con.execute(
        f"COPY (" + songs_sql + f") TO '{SONGS_OUT.as_posix()}' (HEADER, DELIMITER ',')"
    )
    # Write podcasts-only
    con.execute(
        f"COPY (" + pods_sql + f") TO '{POD_OUT.as_posix()}' (HEADER, DELIMITER ',')"
    )

    songs_count = con.execute(
        "SELECT COUNT(*) FROM read_csv_auto(?, ALL_VARCHAR=TRUE, sample_size=-1)",
        [SONGS_OUT.as_posix()],
    ).fetchone()[0]
    pod_count = con.execute(
        "SELECT COUNT(*) FROM read_csv_auto(?, ALL_VARCHAR=TRUE, sample_size=-1)",
        [POD_OUT.as_posix()],
    ).fetchone()[0]
    con.close()
    return songs_count, pod_count


if __name__ == "__main__":
    arg_in = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_IN
    s, p = split_media_types(arg_in)
    print(f"songs_rows={s} podcasts_rows={p}")
