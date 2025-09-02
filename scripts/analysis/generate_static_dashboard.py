#!/usr/bin/env python3
"""
Generate a static, self-contained HTML dashboard from the final viz CSV.

Inputs
  - --input: Path to final CSV (default: ./final_enriched_streaming_history_viz.csv)
  - --output: Output HTML path (default: ./reports/overview.html)

Outputs
  - A single HTML file with embedded charts (no runtime server, no Streamlit).

Notes
  - Uses pandas for aggregation and Altair for charts.
  - Aggregates before plotting to keep HTML small and responsive.
  - Safe to run repeatedly; will overwrite the output file.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import altair as alt


def load_data(path: Path) -> pd.DataFrame:
    df = pd.read_csv(
        path,
        encoding="utf-8",
        encoding_errors="replace",
        low_memory=False,
    )

    # Parse timestamp, coerce errors
    if "ts" in df.columns:
        df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
        df["date"] = df["ts"].dt.date
        df["month"] = df["ts"].dt.to_period("M").dt.to_timestamp()
        df["hour"] = df["ts"].dt.hour
        df["weekday"] = df["ts"].dt.day_name()

    # Ensure minutes_played numeric
    if "minutes_played" in df.columns:
        df["minutes_played"] = pd.to_numeric(df["minutes_played"], errors="coerce").fillna(0.0)
    else:
        df["minutes_played"] = 0.0

    # Normalize names columns that usually exist
    for col in (
        "master_metadata_track_name",
        "master_metadata_album_artist_name",
        "kaggle_genre",
        "api_genres",
    ):
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)

    return df


def chart_plays_over_time(df: pd.DataFrame) -> alt.Chart:
    if "month" not in df.columns:
        return alt.Chart(pd.DataFrame({"note": ["No time column available"]})).mark_text().encode(text="note")

    agg = (
        df.dropna(subset=["month"])
        .groupby("month", as_index=False)["minutes_played"].sum()
        .sort_values("month")
    )
    return (
        alt.Chart(agg, title="Minutes played over time (monthly)")
        .mark_area(line=True)
        .encode(
            x=alt.X("month:T", title="Month"),
            y=alt.Y("minutes_played:Q", title="Minutes played"),
            tooltip=["month:T", alt.Tooltip("minutes_played:Q", format=",")],
        )
        .properties(height=220)
    )


def chart_top_artists(df: pd.DataFrame, top_n: int = 20) -> alt.Chart:
    col_artist = "master_metadata_album_artist_name"
    if col_artist not in df.columns:
        return alt.Chart(pd.DataFrame({"note": ["No artist column available"]})).mark_text().encode(text="note")

    agg = (
        df.groupby(col_artist, as_index=False)["minutes_played"].sum()
        .sort_values("minutes_played", ascending=False)
        .head(top_n)
    )
    return (
        alt.Chart(agg, title=f"Top {top_n} artists by minutes played")
        .mark_bar()
        .encode(
            x=alt.X("minutes_played:Q", title="Minutes played"),
            y=alt.Y(f"{col_artist}:N", sort="-x", title="Artist"),
            tooltip=[col_artist, alt.Tooltip("minutes_played:Q", format=",")],
        )
        .properties(height=22 * len(agg))
    )


def chart_top_tracks(df: pd.DataFrame, top_n: int = 20) -> alt.Chart:
    col_track = "master_metadata_track_name"
    col_artist = "master_metadata_album_artist_name"
    if col_track not in df.columns:
        return alt.Chart(pd.DataFrame({"note": ["No track column available"]})).mark_text().encode(text="note")

    agg = (
        df.groupby([col_track, col_artist], as_index=False)["minutes_played"].sum()
        .sort_values("minutes_played", ascending=False)
        .head(top_n)
    )
    agg["label"] = agg[col_track] + " — " + agg[col_artist].fillna("")

    return (
        alt.Chart(agg, title=f"Top {top_n} tracks by minutes played")
        .mark_bar()
        .encode(
            x=alt.X("minutes_played:Q", title="Minutes played"),
            y=alt.Y("label:N", sort="-x", title="Track"),
            tooltip=["label", alt.Tooltip("minutes_played:Q", format=",")],
        )
        .properties(height=22 * len(agg))
    )


def chart_heatmap_weekday_hour(df: pd.DataFrame) -> alt.Chart:
    if not {"weekday", "hour"}.issubset(df.columns):
        return alt.Chart(pd.DataFrame({"note": ["No weekday/hour columns available"]})).mark_text().encode(text="note")

    # Weekday order
    weekdays = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    dfw = df.copy()
    dfw["weekday"] = pd.Categorical(dfw["weekday"], categories=weekdays, ordered=True)

    agg = (
        dfw.dropna(subset=["weekday", "hour"]).groupby(["weekday", "hour"], as_index=False)["minutes_played"].sum()
    )
    return (
        alt.Chart(agg, title="Listening heatmap (weekday × hour)")
        .mark_rect()
        .encode(
            x=alt.X("hour:O", title="Hour of day"),
            y=alt.Y("weekday:O", title="Weekday"),
            color=alt.Color("minutes_played:Q", title="Minutes", scale=alt.Scale(scheme="blues")),
            tooltip=["weekday", "hour", alt.Tooltip("minutes_played:Q", format=",")],
        )
        .properties(height=220)
    )


def chart_genre_share(df: pd.DataFrame, top_n: int = 15) -> alt.Chart:
    # Combine genres from either api_genres or kaggle_genre if present
    source_col = "api_genres" if "api_genres" in df.columns else ("kaggle_genre" if "kaggle_genre" in df.columns else None)
    if not source_col:
        return alt.Chart(pd.DataFrame({"note": ["No genre columns available"]})).mark_text().encode(text="note")

    genres = (
        df[[source_col, "minutes_played"]]
        .copy()
        .rename(columns={source_col: "genres"})
    )
    # Split comma-separated genres, explode
    genres["genres"] = genres["genres"].fillna("").astype(str)
    genres = genres.assign(genres=genres["genres"].str.split(r",\s*"))
    genres = genres.explode("genres")
    genres["genres"] = genres["genres"].str.strip().str.title()
    genres = genres[genres["genres"] != ""]

    agg = (
        genres.groupby("genres", as_index=False)["minutes_played"].sum()
        .sort_values("minutes_played", ascending=False)
        .head(top_n)
    )
    return (
        alt.Chart(agg, title=f"Top {top_n} genres by minutes played")
        .mark_bar()
        .encode(
            x=alt.X("minutes_played:Q", title="Minutes played"),
            y=alt.Y("genres:N", sort="-x", title="Genre"),
            tooltip=["genres", alt.Tooltip("minutes_played:Q", format=",")],
        )
        .properties(height=22 * len(agg))
    )


def build_html(charts: list[alt.Chart], title: str = "Spotify Listening Overview") -> str:
    # Compose multiple charts vertically using HTML sections; each chart is rendered separately
    sections = []
    for ch in charts:
        # Render each chart to standalone HTML snippet
        html = ch.to_html()
        # Remove DOCTYPE/HTML/BODY wrappers if present to allow concatenation
        # Simple heuristic: extract content between <body>...</body>
        lower = html.lower()
        start = lower.find("<body>")
        end = lower.rfind("</body>")
        body = html[(start + 6) : end] if start != -1 and end != -1 else html
        sections.append(body)

    page = f"""
<!DOCTYPE html>
<html lang=\"en\">
  <head>
    <meta charset=\"utf-8\" />
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
    <title>{title}</title>
    <style>
      body {{
        font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
        margin: 24px;
        color: #111;
      }}
      h1 {{ margin: 0 0 12px 0; }}
      h2 {{ margin: 28px 0 10px 0; }}
      .section {{ margin-bottom: 28px; }}
      .grid {{ display: grid; grid-template-columns: 1fr; gap: 16px; }}
      @media (min-width: 1100px) {{ .grid-2 {{ grid-template-columns: 1fr 1fr; }} }}
      .muted {{ color: #666; font-size: 0.9em; }}
    </style>
  </head>
  <body>
    <h1>{title}</h1>
    <p class=\"muted\">Static report generated with Altair. No server required.</p>
    <div class=\"section\">
      {sections[0] if len(sections) > 0 else ''}
    </div>
    <div class=\"section grid grid-2\">
      {sections[1] if len(sections) > 1 else ''}
      {sections[2] if len(sections) > 2 else ''}
    </div>
    <div class=\"section\">
      {sections[3] if len(sections) > 3 else ''}
    </div>
    <div class=\"section\">
      {sections[4] if len(sections) > 4 else ''}
    </div>
  </body>
  </html>
    """
    return page


def main():
    ap = argparse.ArgumentParser(description="Generate static HTML dashboard from CSV")
    ap.add_argument(
        "--input",
        default=str(Path.cwd() / "final_enriched_streaming_history_viz.csv"),
        help="Path to the final viz CSV",
    )
    ap.add_argument(
        "--output",
        default=str(Path.cwd() / "reports" / "overview.html"),
        help="Path to write the HTML report",
    )
    args = ap.parse_args()

    input_path = Path(args.input).resolve()
    output_path = Path(args.output).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = load_data(input_path)

    charts = [
        chart_plays_over_time(df),
        chart_top_artists(df, 20),
        chart_top_tracks(df, 20),
        chart_heatmap_weekday_hour(df),
        chart_genre_share(df, 15),
    ]

    html = build_html(charts)
    output_path.write_text(html, encoding="utf-8")
    print(f"Wrote: {output_path}")


if __name__ == "__main__":
    main()
