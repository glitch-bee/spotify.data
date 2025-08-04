import pandas as pd
import kagglehub
import os
from tqdm import tqdm
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def download_kaggle_dataset():
    """Download the Spotify tracks dataset from Kaggle"""
    logger.info("Downloading Spotify tracks dataset from Kaggle...")
    try:
        path = kagglehub.dataset_download("maharshipandya/-spotify-tracks-dataset")
        logger.info(f"Dataset downloaded to: {path}")
        return path
    except Exception as e:
        logger.error(f"Failed to download dataset: {e}")
        return None

def load_datasets(kaggle_path):
    """Load both our cleaned data and the Kaggle dataset"""
    logger.info("Loading datasets...")
    
    # Load our cleaned streaming history
    our_data = pd.read_csv("cleaned_streaming_history.csv")
    logger.info(f"Loaded our data: {len(our_data):,} records")
    
    # Find and load the Kaggle dataset file
    kaggle_files = os.listdir(kaggle_path)
    logger.info(f"Kaggle dataset files: {kaggle_files}")
    
    # Look for CSV files in the Kaggle dataset
    csv_files = [f for f in kaggle_files if f.endswith('.csv')]
    if not csv_files:
        logger.error("No CSV files found in Kaggle dataset")
        return None, None
    
    # Load the first CSV file (or specify the correct one)
    kaggle_file = csv_files[0]
    kaggle_data = pd.read_csv(os.path.join(kaggle_path, kaggle_file))
    logger.info(f"Loaded Kaggle data: {len(kaggle_data):,} records from {kaggle_file}")
    logger.info(f"Kaggle dataset columns: {list(kaggle_data.columns)}")
    
    return our_data, kaggle_data

def normalize_string(s):
    """Normalize strings for better matching"""
    if pd.isna(s):
        return ""
    return str(s).lower().strip()

def match_tracks(our_data, kaggle_data):
    """Match tracks between our data and Kaggle dataset"""
    logger.info("Starting track matching...")
    
    # Get unique tracks from our data
    our_unique = our_data[['master_metadata_track_name', 'master_metadata_album_artist_name']].dropna().drop_duplicates()
    logger.info(f"Unique tracks in our data: {len(our_unique):,}")
    
    # Normalize track and artist names for better matching
    our_unique['track_norm'] = our_unique['master_metadata_track_name'].apply(normalize_string)
    our_unique['artist_norm'] = our_unique['master_metadata_album_artist_name'].apply(normalize_string)
    
    # Check what columns are available in Kaggle data for track/artist names
    kaggle_cols = kaggle_data.columns.tolist()
    logger.info(f"Available Kaggle columns: {kaggle_cols}")
    
    # The Kaggle dataset has specific column names
    track_col = 'track_name'
    artist_col = 'artists'
    
    if track_col not in kaggle_cols or artist_col not in kaggle_cols:
        logger.error(f"Expected columns not found. Available: {kaggle_cols}")
        return None
    
    logger.info(f"Using Kaggle columns - Track: {track_col}, Artist: {artist_col}")
    
    # Normalize Kaggle data
    kaggle_data['track_norm'] = kaggle_data[track_col].apply(normalize_string)
    kaggle_data['artist_norm'] = kaggle_data[artist_col].apply(normalize_string)
    
    # Create lookup dictionary from Kaggle data
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
        logger.info(f"Enriched {enriched_count:,} streaming records with Kaggle metadata")
    
    return enriched_data

def main():
    logger.info("Starting Kaggle dataset metadata matching...")
    
    # Download dataset
    kaggle_path = download_kaggle_dataset()
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
    matched_metadata.to_csv("kaggle_matched_metadata.csv", index=False)
    logger.info("Saved matched metadata to kaggle_matched_metadata.csv")
    
    # Merge with full streaming data
    enriched_data = merge_with_streaming_data(our_data, matched_metadata)
    
    # Save enriched data
    enriched_data.to_csv("kaggle_enriched_streaming_history.csv", index=False)
    logger.info("Saved enriched streaming history to kaggle_enriched_streaming_history.csv")
    
    # Report final stats
    logger.info(f"Final dataset: {len(enriched_data):,} records")
    
    # Show what metadata we got
    metadata_cols = [col for col in matched_metadata.columns 
                    if col not in ['master_metadata_track_name', 'master_metadata_album_artist_name', 'track_norm', 'artist_norm']]
    
    if metadata_cols:
        logger.info(f"Available metadata fields: {metadata_cols}")
        for col in metadata_cols:
            non_null_count = enriched_data[col].notna().sum()
            logger.info(f"  {col}: {non_null_count:,} records ({non_null_count/len(enriched_data)*100:.1f}%)")

if __name__ == "__main__":
    main()
