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
PROGRESS_FILE = "data/enriched/api_metadata_progress.pkl"
DELAY_BETWEEN_REQUESTS = 0.3
DELAY_BETWEEN_BATCHES = 5

# Set up Spotipy with Authorization Code Flow (user auth)
scope = "user-library-read"
redirect_uri = "http://127.0.0.1:8888/callback"

auth_manager = SpotifyOAuth(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    redirect_uri=redirect_uri,
    scope=scope,
    cache_path=".spotify_cache"
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
    os.makedirs(os.path.dirname(PROGRESS_FILE), exist_ok=True)
    with open(PROGRESS_FILE, 'wb') as f:
        pickle.dump(processed_tracks, f)

def load_existing_enrichment():
    """Load tracks that already have external metadata to avoid duplicating work"""
    enriched_tracks = set()
    
    # Check for Ultimate Spotify enrichment
    ultimate_file = "data/enriched/ultimate_spotify_enriched_streaming_history.csv"
    if os.path.exists(ultimate_file):
        logger.info("Loading existing Ultimate Spotify enrichment...")
        df = pd.read_csv(ultimate_file)
        
        # Find tracks that have metadata (non-null in key metadata columns)
        metadata_cols = ['acousticness', 'danceability', 'energy', 'valence', 'genre']
        has_metadata = df[metadata_cols].notna().any(axis=1)
        
        enriched_df = df[has_metadata]
        for _, row in enriched_df.iterrows():
            track_key = f"{row['master_metadata_track_name']}|||{row['master_metadata_album_artist_name']}"
            enriched_tracks.add(track_key)
        
        logger.info(f"Found {len(enriched_tracks)} tracks already enriched with external metadata")
    
    return enriched_tracks

def search_track_metadata(track_name, artist_name):
    """Search for a single track and get its metadata (excluding audio features)"""
    query = f"track:{track_name} artist:{artist_name}"
    
    try:
        results = sp.search(q=query, type="track", limit=1)
        items = results["tracks"]["items"]
        
        if not items:
            return None
            
        track_data = items[0]
        track_id = track_data["id"]
        
        # Get genres and artist data
        artist_data = None
        try:
            artist_id = track_data["artists"][0]["id"]
            artist_data = sp.artist(artist_id)
        except Exception as e:
            logger.warning(f"Failed to get artist data for {artist_name}: {e}")
        
        # Extract track and album metadata
        album_data = track_data.get("album", {})
        
        metadata = {
            "spotify_id": track_id,
            "spotify_uri": track_data.get("uri"),
            "track_popularity": track_data.get("popularity"),
            "track_duration_ms": track_data.get("duration_ms"),
            "track_explicit": track_data.get("explicit"),
            "track_preview_url": track_data.get("preview_url"),
            
            # Album metadata
            "album_name": album_data.get("name"),
            "album_release_date": album_data.get("release_date"),
            "album_release_date_precision": album_data.get("release_date_precision"),
            "album_total_tracks": album_data.get("total_tracks"),
            "album_type": album_data.get("album_type"),
            
            # Artist metadata
            "artist_popularity": artist_data.get("popularity") if artist_data else None,
            "artist_followers": artist_data.get("followers", {}).get("total") if artist_data else None,
            "api_genres": ", ".join(artist_data.get("genres", [])) if artist_data else None,
            
            # Note: Audio features skipped due to API 403 errors
            "audio_features_note": "Skipped due to Spotify API limitations",
            "enrichment_source": "spotify_api"
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
                "enrichment_source": "spotify_api"
            })
        
        batch_results.append(result)
    
    return batch_results

def main():
    logger.info("Starting smart Spotify API metadata enrichment...")
    logger.info("This will skip tracks already enriched with external datasets")
    
    # Test authentication first
    try:
        user = sp.current_user()
        logger.info(f"Successfully authenticated as: {user['display_name']} ({user['id']})")
    except Exception as e:
        logger.error(f"Authentication failed: {e}")
        return
    
    # Load cleaned data
    df = pd.read_csv("data/processed/cleaned_streaming_history.csv")
    logger.info(f"Loaded {len(df)} total streaming records")
    
    # Get unique tracks for metadata lookup
    unique_tracks = (
        df[["master_metadata_track_name", "master_metadata_album_artist_name"]]
        .dropna()
        .drop_duplicates()
        .reset_index(drop=True)
    )
    logger.info(f"Found {len(unique_tracks)} unique tracks to process")
    
    # Load tracks that already have external metadata
    already_enriched = load_existing_enrichment()
    
    # Filter out tracks that already have metadata
    tracks_to_process = []
    for _, row in unique_tracks.iterrows():
        track_key = f"{row['master_metadata_track_name']}|||{row['master_metadata_album_artist_name']}"
        if track_key not in already_enriched:
            tracks_to_process.append(row)
    
    tracks_to_process = pd.DataFrame(tracks_to_process)
    logger.info(f"After filtering already enriched tracks: {len(tracks_to_process)} tracks need API enrichment")
    
    if len(tracks_to_process) == 0:
        logger.info("All tracks already have external metadata! No API calls needed.")
        return
    
    # Load previous progress
    processed_tracks = load_progress()
    logger.info(f"Loaded {len(processed_tracks)} previously processed API tracks")
    
    logger.info("NOTE: Skipping audio features due to Spotify API 403 errors")
    logger.info("Will collect: track info, album info, artist info, and genres")
    
    # Process in batches
    all_results = []
    total_batches = (len(tracks_to_process) + BATCH_SIZE - 1) // BATCH_SIZE
    
    try:
        for i in tqdm(range(0, len(tracks_to_process), BATCH_SIZE), desc="Processing batches"):
            batch_num = i // BATCH_SIZE + 1
            batch = tracks_to_process.iloc[i:i + BATCH_SIZE]
            
            logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} tracks)")
            
            batch_results = process_batch(batch, processed_tracks)
            all_results.extend(batch_results)
            
            # Save progress after each batch
            save_progress(processed_tracks)
            
            # Rate limiting between batches
            if i + BATCH_SIZE < len(tracks_to_process):
                logger.info(f"Waiting {DELAY_BETWEEN_BATCHES} seconds before next batch...")
                time.sleep(DELAY_BETWEEN_BATCHES)
    
    except KeyboardInterrupt:
        logger.info("Process interrupted by user. Progress has been saved.")
    except Exception as e:
        logger.error(f"Error during processing: {e}")
        logger.info("Progress has been saved.")
    
    # Create DataFrame and save results
    if all_results:
        results_df = pd.DataFrame(all_results)
        output_file = "data/enriched/spotify_api_metadata.csv"
        results_df.to_csv(output_file, index=False)
        logger.info(f"Saved {len(results_df)} track metadata results to {output_file}")
        
        # Count successful enrichments
        successful = results_df['spotify_id'].notna().sum()
        logger.info(f"Successfully enriched {successful} tracks via API ({successful/len(results_df)*100:.1f}%)")
        
        # Merge with original streaming data for complete enriched dataset
        logger.info("Merging API metadata with streaming history...")
        enriched_data = df.merge(
            results_df,
            on=['master_metadata_track_name', 'master_metadata_album_artist_name'],
            how='left'
        )
        
        final_output = "data/enriched/spotify_api_enriched_streaming_history.csv"
        enriched_data.to_csv(final_output, index=False)
        logger.info(f"Saved complete enriched dataset to {final_output}")
        
        # Final statistics
        api_enriched_count = enriched_data['spotify_id'].notna().sum()
        logger.info(f"Final dataset: {len(enriched_data)} records with {api_enriched_count} API-enriched ({api_enriched_count/len(enriched_data)*100:.1f}%)")
    
    else:
        logger.info("No new tracks were processed.")

if __name__ == "__main__":
    main()
