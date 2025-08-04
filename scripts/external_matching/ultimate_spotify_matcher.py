import pandas as pd
import kagglehub
import os
from tqdm import tqdm
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def download_ultimate_spotify_db():
    """Download the Ultimate Spotify Tracks DB from Kaggle"""
    logger.info("Downloading Ultimate Spotify Tracks DB...")
    try:
        path = kagglehub.dataset_download("zaheenhamidani/ultimate-spotify-tracks-db")
        logger.info(f"Dataset downloaded to: {path}")
        return path
    except Exception as e:
        logger.error(f"Failed to download dataset: {e}")
        return None

def load_datasets(kaggle_path):
    """Load both our cleaned data and the Ultimate Spotify DB"""
    logger.info("Loading datasets...")
    
    # Load our cleaned streaming history
    our_data = pd.read_csv("data/processed/cleaned_streaming_history.csv")
    logger.info(f"Loaded our data: {len(our_data):,} records")
    
    # Load the Ultimate Spotify DB
    kaggle_file = "SpotifyFeatures.csv"
    kaggle_data = pd.read_csv(os.path.join(kaggle_path, kaggle_file))
    logger.info(f"Loaded Ultimate Spotify DB: {len(kaggle_data):,} records")
    logger.info(f"Columns: {list(kaggle_data.columns)}")
    
    return our_data, kaggle_data

def normalize_string(s):
    """Normalize strings for better matching"""
    if pd.isna(s):
        return ""
    return str(s).lower().strip().replace("'", "").replace('"', "")

def match_tracks(our_data, kaggle_data):
    """Match tracks between our data and Ultimate Spotify DB"""
    logger.info("Starting track matching...")
    
    # Get unique tracks from our data
    our_unique = our_data[['master_metadata_track_name', 'master_metadata_album_artist_name']].dropna().drop_duplicates()
    logger.info(f"Unique tracks in our data: {len(our_unique):,}")
    
    # Normalize track and artist names for better matching
    our_unique['track_norm'] = our_unique['master_metadata_track_name'].apply(normalize_string)
    our_unique['artist_norm'] = our_unique['master_metadata_album_artist_name'].apply(normalize_string)
    
    # Normalize Ultimate Spotify DB data
    kaggle_data['track_norm'] = kaggle_data['track_name'].apply(normalize_string)
    kaggle_data['artist_norm'] = kaggle_data['artist_name'].apply(normalize_string)
    
    logger.info("Building lookup dictionary...")
    # Create lookup dictionary from Ultimate Spotify DB
    kaggle_lookup = {}
    for _, row in tqdm(kaggle_data.iterrows(), total=len(kaggle_data), desc="Building lookup"):
        key = (row['track_norm'], row['artist_norm'])
        if key not in kaggle_lookup:  # Keep first match
            kaggle_lookup[key] = row.to_dict()
    
    logger.info(f"Built lookup with {len(kaggle_lookup):,} unique track-artist combinations")
    
    # Match our tracks
    matches = []
    matched_count = 0
    
    for _, row in tqdm(our_unique.iterrows(), total=len(our_unique), desc="Matching tracks"):
        key = (row['track_norm'], row['artist_norm'])
        
        if key in kaggle_lookup:
            match_data = kaggle_lookup[key].copy()
            match_data['master_metadata_track_name'] = row['master_metadata_track_name']
            match_data['master_metadata_album_artist_name'] = row['master_metadata_album_artist_name']
            matches.append(match_data)
            matched_count += 1
        else:
            # No match found
            no_match = {
                'master_metadata_track_name': row['master_metadata_track_name'],
                'master_metadata_album_artist_name': row['master_metadata_album_artist_name']
            }
            matches.append(no_match)
    
    logger.info(f"Matched {matched_count:,} tracks out of {len(our_unique):,} ({matched_count/len(our_unique)*100:.1f}%)")
    
    return pd.DataFrame(matches)

def merge_with_streaming_data(our_data, matched_metadata):
    """Merge the matched metadata back with our full streaming history"""
    logger.info("Merging metadata with streaming history...")
    
    enriched_data = our_data.merge(
        matched_metadata,
        on=['master_metadata_track_name', 'master_metadata_album_artist_name'],
        how='left'
    )
    
    # Count successful enrichments
    metadata_cols = [col for col in matched_metadata.columns 
                    if col not in ['master_metadata_track_name', 'master_metadata_album_artist_name']]
    
    if metadata_cols:
        enriched_count = enriched_data[metadata_cols[0]].notna().sum()
        logger.info(f"Enriched {enriched_count:,} streaming records with Ultimate Spotify DB metadata")
    
    return enriched_data

def main():
    logger.info("Starting Ultimate Spotify DB metadata matching...")
    
    # Download dataset
    kaggle_path = download_ultimate_spotify_db()
    if not kaggle_path:
        return
    
    # Load datasets
    our_data, kaggle_data = load_datasets(kaggle_path)
    if our_data is None or kaggle_data is None:
        return
    
    # Match tracks
    matched_metadata = match_tracks(our_data, kaggle_data)
    if matched_metadata is None:
        return
    
    # Save matched metadata
    os.makedirs("external_datasets/ultimate_spotify_db", exist_ok=True)
    matched_metadata.to_csv("external_datasets/ultimate_spotify_db/matched_metadata.csv", index=False)
    logger.info("Saved matched metadata to external_datasets/ultimate_spotify_db/matched_metadata.csv")
    
    # Merge with full streaming data
    enriched_data = merge_with_streaming_data(our_data, matched_metadata)
    
    # Save enriched data
    os.makedirs("data/enriched", exist_ok=True)
    enriched_data.to_csv("data/enriched/ultimate_spotify_enriched_streaming_history.csv", index=False)
    logger.info("Saved enriched streaming history to data/enriched/ultimate_spotify_enriched_streaming_history.csv")
    
    # Report final stats
    logger.info(f"Final dataset: {len(enriched_data):,} records")
    
    # Show what metadata we got
    audio_features = ['danceability', 'energy', 'valence', 'tempo', 'acousticness', 'instrumentalness', 'liveness', 'loudness', 'speechiness']
    metadata_cols = ['genre', 'popularity', 'duration_ms'] + audio_features
    
    if any(col in enriched_data.columns for col in metadata_cols):
        logger.info("Available metadata fields and coverage:")
        for col in metadata_cols:
            if col in enriched_data.columns:
                non_null_count = enriched_data[col].notna().sum()
                logger.info(f"  {col}: {non_null_count:,} records ({non_null_count/len(enriched_data)*100:.1f}%)")
        
        # Show genre distribution
        if 'genre' in enriched_data.columns:
            logger.info("Top genres in matched data:")
            genre_counts = enriched_data['genre'].value_counts().head(10)
            for genre, count in genre_counts.items():
                logger.info(f"  {genre}: {count:,} plays")

if __name__ == "__main__":
    main()
