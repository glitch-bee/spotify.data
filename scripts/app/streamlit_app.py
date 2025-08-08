import os
from pathlib import Path
import pandas as pd
import numpy as np
import streamlit as st
import altair as alt
from datetime import datetime

ROOT = Path(__file__).resolve().parents[2]

FINAL_FILE = ROOT / "data/enriched/final_enriched_streaming_history.csv"
API_FILE = ROOT / "data/enriched/spotify_api_enriched_streaming_history.csv"
KAGGLE_FILE = ROOT / "data/enriched/ultimate_spotify_enriched_streaming_history.csv"
CLEAN_FILE = ROOT / "data/processed/cleaned_streaming_history.csv"

DATE_COL_CANDIDATES = ["ts", "end_time", "timestamp"]
MINUTES_COL_CANDIDATES = ["minutes_played", "ms_played"]
GENRE_COL_CANDIDATES = ["genre", "genres"]

JOIN_KEYS = ["master_metadata_track_name", "master_metadata_album_artist_name"]

@st.cache_data(show_spinner=False)
def load_data():
    # Prefer final merged dataset
    if FINAL_FILE.exists():
        df = pd.read_csv(FINAL_FILE)
    elif KAGGLE_FILE.exists() or API_FILE.exists():
        # Fallback: best available enriched
        base = pd.read_csv(CLEAN_FILE) if CLEAN_FILE.exists() else None
        frames = []
        if base is not None:
            frames.append(base)
        if KAGGLE_FILE.exists():
            frames.append(pd.read_csv(KAGGLE_FILE))
        if API_FILE.exists():
            frames.append(pd.read_csv(API_FILE))
        # Try to merge incrementally on JOIN_KEYS
        df = frames[0]
        for f in frames[1:]:
            common_cols = [c for c in f.columns if c in JOIN_KEYS]
            if set(common_cols) == set(JOIN_KEYS):
                df = df.merge(f, on=JOIN_KEYS, how="left", suffixes=("", "_dup"))
            else:
                # If not mergeable, prefer the bigger table
                if len(f) > len(df):
                    df = f
    else:
        df = pd.read_csv(CLEAN_FILE)

    # Date column
    date_col = next((c for c in DATE_COL_CANDIDATES if c in df.columns), None)
    if date_col is None:
        # Recompose from parts if present
        if {"year","month","day"}.issubset(df.columns):
            df["date"] = pd.to_datetime(dict(year=df.year, month=df.month, day=df.day))
        else:
            st.error("Could not locate a timestamp or year/month/day columns.")
            st.stop()
    else:
        # parse with timezone if present
        df["date"] = pd.to_datetime(df[date_col], errors="coerce")
        df = df.dropna(subset=["date"]).copy()
        df["date"] = df["date"].dt.tz_convert(None) if df["date"].dt.tz is not None else df["date"]

    # Minutes played
    minutes_col = next((c for c in MINUTES_COL_CANDIDATES if c in df.columns), None)
    if minutes_col == "minutes_played":
        df["minutes"] = pd.to_numeric(df[minutes_col], errors="coerce").fillna(0)
    elif minutes_col == "ms_played":
        df["minutes"] = pd.to_numeric(df[minutes_col], errors="coerce").fillna(0) / 60000.0
    else:
        df["minutes"] = 0.0

    # Genre
    genre_col = next((c for c in GENRE_COL_CANDIDATES if c in df.columns), None)
    if genre_col:
        df["genre_std"] = df[genre_col].astype(str).str.split(",").str[0].str.strip()
    else:
        df["genre_std"] = "Unknown"

    # Artist / Track names
    if "master_metadata_album_artist_name" in df.columns:
        df["artist"] = df["master_metadata_album_artist_name"].astype(str)
    elif "artist_name" in df.columns:
        df["artist"] = df["artist_name"].astype(str)
    else:
        df["artist"] = ""

    if "master_metadata_track_name" in df.columns:
        df["track"] = df["master_metadata_track_name"].astype(str)
    elif "track_name" in df.columns:
        df["track"] = df["track_name"].astype(str)
    else:
        df["track"] = ""

    # Weekday/hour
    df["weekday"] = df["date"].dt.day_name()
    df["dow"] = df["date"].dt.weekday
    df["hour"] = df["date"].dt.hour
    df["day"] = df["date"].dt.date
    df["month"] = df["date"].dt.to_period("M").dt.to_timestamp()

    # Audio features if available
    for col in ["energy","valence","danceability","tempo","acousticness"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def kpi_cards(df):
    total_min = df["minutes"].sum()
    total_days = df["day"].nunique()
    total_tracks = df[["track","artist"]].dropna().drop_duplicates().shape[0]
    st.metric("Total minutes", f"{total_min:,.0f}")
    st.metric("Listening days", f"{total_days:,}")
    st.metric("Unique tracks", f"{total_tracks:,}")


def daily_area(df):
    daily = df.groupby("day", as_index=False)["minutes"].sum()
    chart = (
        alt.Chart(daily)
        .mark_area(opacity=0.5)
        .encode(x="day:T", y=alt.Y("minutes:Q", title="Minutes"))
        .properties(height=180)
    )
    st.altair_chart(chart, use_container_width=True)


def hour_weekday_heatmap(df):
    heat = df.groupby(["dow","hour"], as_index=False)["minutes"].sum()
    heat["weekday"] = heat["dow"].map({0:"Mon",1:"Tue",2:"Wed",3:"Thu",4:"Fri",5:"Sat",6:"Sun"})
    chart = (
        alt.Chart(heat)
        .mark_rect()
        .encode(
            x=alt.X("hour:O", title="Hour"),
            y=alt.Y("weekday:O", sort=["Mon","Tue","Wed","Thu","Fri","Sat","Sun"], title="Weekday"),
            color=alt.Color("minutes:Q", scale=alt.Scale(scheme="blues"), title="Minutes"),
            tooltip=["weekday","hour","minutes"],
        )
        .properties(height=220)
    )
    st.altair_chart(chart, use_container_width=True)


def monthly_by_genre(df, top_n=8):
    top_genres = (
        df.groupby("genre_std")["minutes"].sum().sort_values(ascending=False).head(top_n).index.tolist()
    )
    sub = df[df["genre_std"].isin(top_genres)]
    monthly = sub.groupby(["month","genre_std"], as_index=False)["minutes"].sum()
    chart = (
        alt.Chart(monthly)
        .mark_area()
        .encode(
            x="month:T",
            y=alt.Y("minutes:Q", stack=True, title="Minutes"),
            color=alt.Color("genre_std:N", legend=alt.Legend(title="Genre")),
            tooltip=["month","genre_std","minutes"],
        )
        .properties(height=220)
    )
    st.altair_chart(chart, use_container_width=True)


def rolling_features(df):
    feats = [c for c in ["energy","valence","danceability","acousticness"] if c in df.columns]
    if not feats:
        st.info("Audio features not available in current dataset.")
        return
    daily = df.groupby("day", as_index=False)[feats].mean().sort_values("day")
    for c in feats:
        daily[c+"_roll"] = daily[c].rolling(30, min_periods=7).mean()
    melted = daily.melt("day", value_vars=[c+"_roll" for c in feats], var_name="feature", value_name="value")
    melted["feature"] = melted["feature"].str.replace("_roll","", regex=False)
    chart = (
        alt.Chart(melted)
        .mark_line()
        .encode(x="day:T", y=alt.Y("value:Q", title="30d rolling mean"), color="feature:N")
        .properties(height=220)
    )
    st.altair_chart(chart, use_container_width=True)


def top_table(df):
    by_artist = df.groupby("artist", as_index=False)["minutes"].sum().sort_values("minutes", ascending=False).head(20)
    by_track = df.groupby(["track","artist"], as_index=False)["minutes"].sum().sort_values("minutes", ascending=False).head(20)
    st.subheader("Top artists (by minutes)")
    st.dataframe(by_artist, use_container_width=True, hide_index=True)
    st.subheader("Top tracks (by minutes)")
    st.dataframe(by_track, use_container_width=True, hide_index=True)


def main():
    st.set_page_config(page_title="Spotify Listening Dashboard", layout="wide")
    st.title("Spotify Listening Dashboard")

    with st.spinner("Loading data..."):
        df = load_data()

    # Sidebar filters
    st.sidebar.header("Filters")
    min_date, max_date = df["date"].min(), df["date"].max()
    date_range = st.sidebar.date_input("Date range", (min_date.date(), max_date.date()))
    if isinstance(date_range, tuple):
        start, end = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1]) + pd.Timedelta(days=1)
        df = df[(df["date"] >= start) & (df["date"] < end)]

    # Genre filter
    genres = sorted([g for g in df["genre_std"].dropna().unique() if g and g != "nan"])[:2000]
    selected_genres = st.sidebar.multiselect("Genres", options=genres, default=None)
    if selected_genres:
        df = df[df["genre_std"].isin(selected_genres)]

    # Artist search
    artist_query = st.sidebar.text_input("Artist contains")
    if artist_query:
        df = df[df["artist"].str.contains(artist_query, case=False, na=False)]

    # Layout
    c1, c2, c3 = st.columns(3)
    with c1: kpi_cards(df)
    with c2: st.empty()
    with c3: st.empty()

    st.subheader("Daily listening minutes")
    daily_area(df)

    st.subheader("Listening by hour x weekday")
    hour_weekday_heatmap(df)

    st.subheader("Monthly listening by genre (top 8)")
    monthly_by_genre(df)

    st.subheader("Audio features (30-day rolling means)")
    rolling_features(df)

    st.subheader("Top artists and tracks")
    top_table(df)

    st.caption("Tips: Use the sidebar to filter by date, genre, and artist. If audio features are missing, run Kaggle enrichment to populate them.")


if __name__ == "__main__":
    main()
