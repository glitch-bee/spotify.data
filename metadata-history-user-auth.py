import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import time
import os
import pickle
from tqdm import tqdm
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Your credentials from environment variables
CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")

if not CLIENT_ID or not CLIENT_SECRET:
    raise ValueError("Please set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET in your .env file")

# Configuration
BATCH_SIZE = 50
PROGRESS_FILE = "metadata_progress.pkl"
DELAY_BETWEEN_REQUESTS = 0.3
DELAY_BETWEEN_BATCHES = 5

# Set up Spotipy with Authorization Code Flow (user auth)
# This requires a one-time browser login but then works reliably
scope = "user-library-read"  # Minimal scope needed
redirect_uri = "http://127.0.0.1:8888/callback"  # This needs to be added to your Spotify app settings

auth_manager = SpotifyOAuth(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    redirect_uri=redirect_uri,
    scope=scope,
    cache_path=".spotify_cache"  # This will store the token locally
)

sp = spotipy.Spotify(auth_manager=auth_manager)

def load_progress():
    """Load previously processed tracks to avoid re-processing"""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'rb') as f:
            return pickle.load(f)
    return {}

def save_progress(processed_tracks):
    """Save progress to resume later if needed"""
    with open(PROGRESS_FILE, 'wb') as f:
        pickle.dump(processed_tracks, f)

def search_track_metadata(track_name, artist_name):
    """Search for a single track and get its metadata"""
    query = f"track:{track_name} artist:{artist_name}"
    
    try:
        results = sp.search(q=query, type="track", limit=1)
        items = results["tracks"]["items"]
        
        if not items:
            return None
            
        track_data = items[0]
        track_id = track_data["id"]
        
        # Get audio features (should work now with user auth)
        features = None
        try:
            features_list = sp.audio_features([track_id])
            features = features_list[0] if features_list else None
        except Exception as e:
            logger.warning(f"Failed to get audio features for {track_name}: {e}")
        
        # Get genres from artist
        artist_data = None
        try:
            artist_id = track_data["artists"][0]["id"]
            artist_data = sp.artist(artist_id)
        except Exception as e:
            logger.warning(f"Failed to get artist data for {artist_name}: {e}")
        
        # Extract additional track metadata
        album_data = track_data.get("album", {})
        
        metadata = {
            "spotify_id": track_id,
            "spotify_uri": track_data.get("uri"),
            "track_popularity": track_data.get("popularity"),
            "album_name": album_data.get("name"),
            "album_release_date": album_data.get("release_date"),
            "album_total_tracks": album_data.get("total_tracks"),
            "track_duration_ms": track_data.get("duration_ms"),
            "track_explicit": track_data.get("explicit"),
            "track_preview_url": track_data.get("preview_url"),
            
            # Audio features (if available)
            "danceability": features.get("danceability") if features else None,
            "energy": features.get("energy") if features else None,
            "valence": features.get("valence") if features else None,
            "tempo": features.get("tempo") if features else None,
            "acousticness": features.get("acousticness") if features else None,
            "instrumentalness": features.get("instrumentalness") if features else None,
            "speechiness": features.get("speechiness") if features else None,
            "liveness": features.get("liveness") if features else None,
            "loudness": features.get("loudness") if features else None,
            "key": features.get("key") if features else None,
            "mode": features.get("mode") if features else None,
            "time_signature": features.get("time_signature") if features else None,
            
            # Artist data
            "artist_popularity": artist_data.get("popularity") if artist_data else None,
            "artist_followers": artist_data.get("followers", {}).get("total") if artist_data else None,
            "genres": ", ".join(artist_data.get("genres", [])) if artist_data else None
        }
        
        return metadata
        
    except Exception as e:
        logger.warning(f"Error fetching {track_name} by {artist_name}: {e}")
        return None

def process_batch(tracks_batch, processed_tracks):
    """Process a batch of tracks"""
    batch_results = []
    
    for _, row in tracks_batch.iterrows():
        track = row["master_metadata_track_name"]
        artist = row["master_metadata_album_artist_name"]
        
        # Create a unique key for this track
        track_key = f"{track}|||{artist}"
        
        # Skip if already processed
        if track_key in processed_tracks:
            metadata = processed_tracks[track_key]
        else:
            metadata = search_track_metadata(track, artist)
            processed_tracks[track_key] = metadata
            time.sleep(DELAY_BETWEEN_REQUESTS)
        
        # Add the track info and metadata to results
        result = {
            "master_metadata_track_name": track,
            "master_metadata_album_artist_name": artist
        }
        
        if metadata:
            result.update(metadata)
        else:
            # Add None values for failed lookups
            result.update({
                "spotify_id": None,
                "spotify_uri": None,
                "track_popularity": None,
                "album_name": None,
                "album_release_date": None,
                "album_total_tracks": None,
                "track_duration_ms": None,
                "track_explicit": None,
                "track_preview_url": None,
                "danceability": None,
                "energy": None,
                "valence": None,
                "tempo": None,
                "acousticness": None,
                "instrumentalness": None,
                "speechiness": None,
                "liveness": None,
                "loudness": None,
                "key": None,
                "mode": None,
                "time_signature": None,
                "artist_popularity": None,
                "artist_followers": None,
                "genres": None
            })
        
        batch_results.append(result)
    
    return batch_results

def main():
    logger.info("Starting Spotify metadata enrichment with user authentication...")
    
    # Test authentication first
    try:
        user = sp.current_user()
        logger.info(f"Successfully authenticated as: {user['display_name']} ({user['id']})")
    except Exception as e:
        logger.error(f"Authentication failed: {e}")
        logger.error("Please make sure to:")
        logger.error("1. Add http://127.0.0.1:8888/callback to your Spotify app's Redirect URIs")
        logger.error("2. Complete the browser authentication when prompted")
        return
    
    # Load cleaned data
    df = pd.read_csv("cleaned_streaming_history.csv")
    logger.info(f"Loaded {len(df)} total streaming records")
    
    # Get unique tracks for metadata lookup
    unique_tracks = (
        df[["master_metadata_track_name", "master_metadata_album_artist_name"]]
        .dropna()
        .drop_duplicates()
        .reset_index(drop=True)
    )
    logger.info(f"Found {len(unique_tracks)} unique tracks to process")
    
    # Load previous progress
    processed_tracks = load_progress()
    logger.info(f"Loaded {len(processed_tracks)} previously processed tracks")
    
    # Process in batches
    all_results = []
    total_batches = (len(unique_tracks) + BATCH_SIZE - 1) // BATCH_SIZE
    
    try:
        for i in tqdm(range(0, len(unique_tracks), BATCH_SIZE), desc="Processing batches"):
            batch_num = i // BATCH_SIZE + 1
            logger.info(f"Processing batch {batch_num}/{total_batches}")
            
            batch = unique_tracks.iloc[i:i+BATCH_SIZE]
            batch_results = process_batch(batch, processed_tracks)
            all_results.extend(batch_results)
            
            # Save progress after each batch
            save_progress(processed_tracks)
            
            # Rest between batches (except for the last one)
            if i + BATCH_SIZE < len(unique_tracks):
                logger.info(f"Resting {DELAY_BETWEEN_BATCHES} seconds before next batch...")
                time.sleep(DELAY_BETWEEN_BATCHES)
                
    except KeyboardInterrupt:
        logger.info("Process interrupted by user. Progress has been saved.")
        
    except Exception as e:
        logger.error(f"Process failed: {e}")
        logger.info("Progress has been saved and can be resumed.")
    
    if all_results:
        # Create DataFrame with metadata
        metadata_df = pd.DataFrame(all_results)
        
        # Merge metadata back into main dataframe
        logger.info("Merging metadata with original data...")
        enriched_df = df.merge(
            metadata_df,
            on=["master_metadata_track_name", "master_metadata_album_artist_name"],
            how="left",
        )
        
        # Save to final CSV
        output_file = "enriched_streaming_history.csv"
        enriched_df.to_csv(output_file, index=False)
        logger.info(f"Saved enriched data to {output_file}")
        
        # Report success stats
        successful_enrichments = metadata_df['spotify_id'].notna().sum()
        audio_features_count = metadata_df['danceability'].notna().sum()
        genres_count = metadata_df['genres'].notna().sum()
        
        logger.info(f"Final dataset has {len(enriched_df)} records")
        logger.info(f"Successfully enriched: {successful_enrichments} tracks")
        logger.info(f"Audio features obtained: {audio_features_count} tracks")
        logger.info(f"Genres obtained: {genres_count} tracks")
        
        # Clean up progress file if completed successfully
        if len(all_results) == len(unique_tracks):
            if os.path.exists(PROGRESS_FILE):
                os.remove(PROGRESS_FILE)
                logger.info("Process completed! Cleaned up progress file.")

if __name__ == "__main__":
    main()
