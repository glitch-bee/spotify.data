import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import time
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Your credentials from environment variables
CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")

if not CLIENT_ID or not CLIENT_SECRET:
    raise ValueError("Please set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET in your .env file")

# Set up Spotipy auth
auth_manager = SpotifyClientCredentials(
    client_id=CLIENT_ID, client_secret=CLIENT_SECRET
)
sp = spotipy.Spotify(auth_manager=auth_manager)

# Load cleaned data
df = pd.read_csv("cleaned_streaming_history.csv")

# Remove duplicates for lookup efficiency
unique_tracks = (
    df[["master_metadata_track_name", "master_metadata_album_artist_name"]]
    .dropna()
    .drop_duplicates()
)
unique_tracks["spotify_id"] = None
unique_tracks["genres"] = None
unique_tracks["danceability"] = None
unique_tracks["energy"] = None
unique_tracks["valence"] = None
unique_tracks["tempo"] = None

# Search for track and get metadata
for idx, row in unique_tracks.iterrows():
    track = row["master_metadata_track_name"]
    artist = row["master_metadata_album_artist_name"]
    query = f"track:{track} artist:{artist}"

    try:
        results = sp.search(q=query, type="track", limit=1)
        items = results["tracks"]["items"]
        if items:
            track_id = items[0]["id"]
            unique_tracks.at[idx, "spotify_id"] = track_id

            # Get audio features
            features = sp.audio_features([track_id])[0]
            if features:
                unique_tracks.at[idx, "danceability"] = features["danceability"]
                unique_tracks.at[idx, "energy"] = features["energy"]
                unique_tracks.at[idx, "valence"] = features["valence"]
                unique_tracks.at[idx, "tempo"] = features["tempo"]

            # Get genres from artist
            artist_id = items[0]["artists"][0]["id"]
            artist_data = sp.artist(artist_id)
            if artist_data and "genres" in artist_data:
                unique_tracks.at[idx, "genres"] = ", ".join(artist_data["genres"])

    except Exception as e:
        print(f"Error fetching {track} by {artist}: {e}")
        continue

    time.sleep(0.1)  # Avoid rate limits

# Merge metadata back into main dataframe
df = df.merge(
    unique_tracks,
    on=["master_metadata_track_name", "master_metadata_album_artist_name"],
    how="left",
)

# Save to final CSV
df.to_csv("enriched_streaming_history.csv", index=False)
print("Saved enriched data to enriched_streaming_history.csv")
