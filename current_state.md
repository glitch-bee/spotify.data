# Current State (Aug 31, 2025)

This document is the single source of truth for the Spotify data project: what exists, what’s canonical, how to run things, and what’s left.

## Snapshot
- Canonical dataset (for analysis/visualization):
  - data/enriched/spotify_api_enriched_streaming_history.csv (songs+podcasts)
  - Optional splits:
    - data/enriched/spotify_api_enriched_streaming_history_songs.csv
    - data/enriched/spotify_api_enriched_streaming_history_podcasts.csv
- External-only enrichment (reference):
  - data/enriched/ultimate_spotify_enriched_streaming_history.csv (Kaggle)
- Progress DB (resume-safe API enrichment):
  - data/enriched/api_progress.sqlite
- Raw API metadata cache (append-only):
  - data/enriched/spotify_api_metadata.csv
- Base cleaned history (input):
  - data/processed/cleaned_streaming_history.csv

Note: A future, single “all-sources” final file may be written as data/enriched/final_enriched_streaming_history.csv. It does not exist yet; until then, use the API-enriched file as canonical.

## Latest coverage (as of Aug 31, 2025)
- Unique base keys: 27,265
- Kaggle-matched: 6,069
- API-completed: 19,898
- Remaining unique keys: ~1,298
- Line counts:
  - API metadata rows: 29,869 (incl. header)
  - Final merged rows: 138,762
  - Songs split rows: 127,099
  - Podcasts split rows: 11,664

## Pipeline overview
- Multi-source enrichment with skip/resume logic:
  - Skip anything already matched by Kaggle (requires reliable signals like track_id/popularity),
  - Skip API keys already processed (persisted in SQLite),
  - Stream API results to CSV to avoid memory spikes,
  - On-disk LEFT JOIN via DuckDB; dedup meta per (track, artist) with ROW_NUMBER().
- Normalized join keys: lower(trim(track_name)), lower(trim(artist_name)).
- Paths are rooted under data/, with a venv at .venv for runs.

## Key scripts
- scripts/spotify_api/smart_metadata_enrichment.py
  - Enrich via Spotify API with resumable progress (SQLite) and append-only CSV for metadata.
  - Also supports merge-only mode to refresh the final merged CSV from cached metadata.
- scripts/spotify_api/duckdb_merge.py
  - On-disk LEFT JOIN of base × metadata; robust CSV parsing; dedups.
- scripts/spotify_api/split_media_types.py
  - Produces songs/podcasts CSV splits from the merged file.
- scripts/external_matching/ultimate_spotify_matcher.py (in use)
- scripts/external_matching/kaggle_metadata_match.py (legacy)
- scripts/enrichment/*.py (legacy)

## How to run (minimal)
Using the existing venv at .venv.

- Enrichment (resume-safe):
  - Populates data/enriched/spotify_api_metadata.csv and updates data/enriched/api_progress.sqlite.
  - Merge-only mode can be used after an API run to refresh the final CSV fast.

- Merge + split-only (no API calls):
  - Updates data/enriched/spotify_api_enriched_streaming_history.csv and the songs/podcasts splits.

- Dashboard (Streamlit):
  - Loads a single canonical source (prefers final if present; otherwise API-enriched; else base cleaned). Includes a media type filter (Songs/Podcasts).

## Decisions and guardrails
- Canonical for analysis: data/enriched/spotify_api_enriched_streaming_history.csv until final_enriched_streaming_history.csv exists.
- No in-app multi-CSV merges in the dashboard (to avoid memory spikes). Merge is done offline via DuckDB scripts.
- Rate-limit safe API runs with persistent progress; safe to resume after daily quota resets.

## Known issues
- Legacy artifacts present: scripts/enrichment/*, scripts/external_matching/kaggle_metadata_match.py, data/enriched/api_metadata_progress.pkl.
- .gitignore likely misformatted and over-broad; needs cleanup.
- Streamlit can be heavy on very large ranges; the app was simplified to read only one dataset and only the needed columns.

## Next steps
1) Final pass of API enrichment to cover the last ~1,298 unique keys; then run merge-only and refresh splits.
2) Produce a true single final file at data/enriched/final_enriched_streaming_history.csv via the DuckDB merge.
3) Light repo cleanup:
   - Remove/retire legacy scripts and the old progress .pkl after verifying no references remain,
   - Fix .gitignore (remove code block fences; narrow patterns),
   - Ensure README references the canonical file and updated run steps.
4) Optional: Add refined second-pass matching for stubborn leftovers and add a few more charts (release year vs plays, “new vs repeat” plays) in the dashboard.

---
This document is intended to be the fresh starting point for future work and chats. Use the API-enriched CSV as the canonical analysis source until a final merged file is minted.
