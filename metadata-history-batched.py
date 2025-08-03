import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
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
BATCH_SIZE = 50   # Smaller batches to be more conservative
PROGRESS_FILE = "metadata_progress.pkl"  # Save progress between runs
DELAY_BETWEEN_REQUESTS = 0.5  # Longer delay between API calls
DELAY_BETWEEN_BATCHES = 10  # Longer rest between batches

# Set up Spotipy auth
auth_manager = SpotifyClientCredentials(
    client_id=CLIENT_ID, client_secret=CLIENT_SECRET
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
            
        track_id = items[0]["id"]
        
        # Get audio features with rate limit handling
        try:
            features = sp.audio_features([track_id])[0]
        except Exception as e:
            if "403" in str(e) or "rate limit" in str(e).lower():
                logger.warning(f"Rate limit hit for audio features. Waiting 60 seconds...")
                time.sleep(60)
                try:
                    features = sp.audio_features([track_id])[0]
                except:
                    features = None
            else:
                features = None
        
        # Get genres from artist with rate limit handling
        try:
            artist_id = items[0]["artists"][0]["id"]
            artist_data = sp.artist(artist_id)
        except Exception as e:
            if "403" in str(e) or "rate limit" in str(e).lower():
                logger.warning(f"Rate limit hit for artist data. Waiting 60 seconds...")
                time.sleep(60)
                try:
                    artist_data = sp.artist(artist_id)
                except:
                    artist_data = None
            else:
                artist_data = None
        
        metadata = {
            "spotify_id": track_id,
            "danceability": features["danceability"] if features else None,
            "energy": features["energy"] if features else None,
            "valence": features["valence"] if features else None,
            "tempo": features["tempo"] if features else None,
            "acousticness": features["acousticness"] if features else None,
            "instrumentalness": features["instrumentalness"] if features else None,
            "speechiness": features["speechiness"] if features else None,
            "liveness": features["liveness"] if features else None,
            "loudness": features["loudness"] if features else None,
            "genres": ", ".join(artist_data["genres"]) if artist_data and "genres" in artist_data else None
        }
        
        return metadata
        
    except Exception as e:
        if "403" in str(e) or "rate limit" in str(e).lower():
            logger.error(f"Rate limit exceeded. You may need to wait longer before retrying.")
            raise e
        else:
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
                "danceability": None,
                "energy": None,
                "valence": None,
                "tempo": None,
                "acousticness": None,
                "instrumentalness": None,
                "speechiness": None,
                "liveness": None,
                "loudness": None,
                "genres": None
            })
        
        batch_results.append(result)
    
    return batch_results

def main():
    logger.info("Starting Spotify metadata enrichment process...")
    
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
    logger.info(f"Final dataset has {len(enriched_df)} records with {metadata_df['spotify_id'].notna().sum()} successfully enriched tracks")
    
    # Clean up progress file
    if os.path.exists(PROGRESS_FILE):
        os.remove(PROGRESS_FILE)
        logger.info("Cleaned up progress file")

if __name__ == "__main__":
    main()
