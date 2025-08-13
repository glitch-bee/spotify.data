"""DuckDB merge helper for Spotify enrichment.

Deps:
    pip install duckdb
"""

from __future__ import annotations

from pathlib import Path
import logging

import duckdb

logger = logging.getLogger(__name__)


def run_duckdb_merge(base_csv: Path, meta_csv: Path, out_csv: Path):
    """LEFT JOIN base with meta using normalized keys, writing to out_csv.
    Returns (rows_written, meta_distinct_rows)."""

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    if out_csv.exists():
        out_csv.unlink()

    con = duckdb.connect()

    query = f"""
    WITH
    base AS (
        SELECT *, lower(trim(master_metadata_track_name)) AS tkey,
                   lower(trim(master_metadata_album_artist_name)) AS akey
        FROM read_csv_auto('{base_csv.as_posix()}', ALL_VARCHAR=TRUE, filename=true)
    ),
    meta_raw AS (
        SELECT *, lower(trim(master_metadata_track_name)) AS tkey,
                   lower(trim(master_metadata_album_artist_name)) AS akey
        FROM read_csv_auto('{meta_csv.as_posix()}', ALL_VARCHAR=TRUE, filename=true)
    ),
    meta_dedup AS (
        SELECT * EXCLUDE rn FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY tkey, akey ORDER BY spotify_id DESC NULLS LAST) AS rn
            FROM meta_raw
        ) WHERE rn = 1
    )
    SELECT b.*, m.spotify_id, m.spotify_uri, m.track_popularity, m.track_duration_ms,
           m.track_explicit, m.track_preview_url, m.album_name, m.album_release_date,
           m.album_release_date_precision, m.album_total_tracks, m.album_type,
           m.artist_popularity, m.artist_followers, m.api_genres, m.audio_features_note,
           m.enrichment_source
    FROM base b
    LEFT JOIN meta_dedup m
      ON b.tkey = m.tkey AND b.akey = m.akey
    """

    con.execute(f"COPY (" + query + f") TO '{out_csv.as_posix()}' (HEADER, DELIMITER ',')")

    dupes = con.execute(
        """
        WITH meta_raw AS (
            SELECT lower(trim(master_metadata_track_name)) AS tkey,
                   lower(trim(master_metadata_album_artist_name)) AS akey
            FROM read_csv_auto(?, ALL_VARCHAR=TRUE)
        )
        SELECT COUNT(*) FROM (
            SELECT tkey, akey, COUNT(*) c FROM meta_raw GROUP BY tkey, akey HAVING c > 1
        )
        """,
        [meta_csv.as_posix()],
    ).fetchone()[0]
    if dupes:
        logger.info(f"Meta duplicates dropped={dupes}")

    meta_count = con.execute(
        "SELECT COUNT(*) FROM read_csv_auto(?, ALL_VARCHAR=TRUE)", [meta_csv.as_posix()]
    ).fetchone()[0]
    rows_count = con.execute(
        "SELECT COUNT(*) FROM read_csv_auto(?, ALL_VARCHAR=TRUE)", [out_csv.as_posix()]
    ).fetchone()[0]
    con.close()
    return rows_count, meta_count

