import pandas as pd
import os
from datetime import datetime

def analyze_coverage():
    """Analyze current metadata enrichment coverage from all sources"""
    
    print("=" * 60)
    print("SPOTIFY DATA ENRICHMENT COVERAGE ANALYSIS")
    print("=" * 60)
    print(f"Analysis run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Load base cleaned data
    base_data = pd.read_csv("data/processed/cleaned_streaming_history.csv")
    total_records = len(base_data)
    unique_tracks = base_data[['master_metadata_track_name', 'master_metadata_album_artist_name']].dropna().drop_duplicates()
    total_unique_tracks = len(unique_tracks)
    
    print(f"📊 BASE DATASET:")
    print(f"   Total streaming records: {total_records:,}")
    print(f"   Unique tracks: {total_unique_tracks:,}")
    print()
    
    coverage_summary = []
    
    # 1. Kaggle/External Dataset Coverage
    kaggle_file = "data/enriched/ultimate_spotify_enriched_streaming_history.csv"
    if os.path.exists(kaggle_file):
        print("🎵 KAGGLE/EXTERNAL DATASET COVERAGE:")
        kaggle_data = pd.read_csv(kaggle_file)
        
        # Check for any metadata columns (audio features)
        metadata_cols = ['acousticness', 'danceability', 'energy', 'valence']
        has_metadata = kaggle_data[metadata_cols].notna().any(axis=1)
        kaggle_enriched = has_metadata.sum()
        kaggle_pct = (kaggle_enriched / total_records) * 100
        
        print(f"   Records with Kaggle metadata: {kaggle_enriched:,} ({kaggle_pct:.1f}%)")
        
        # Unique tracks with Kaggle metadata
        kaggle_unique = kaggle_data[has_metadata][['master_metadata_track_name', 'master_metadata_album_artist_name']].drop_duplicates()
        kaggle_unique_count = len(kaggle_unique)
        kaggle_unique_pct = (kaggle_unique_count / total_unique_tracks) * 100
        print(f"   Unique tracks with metadata: {kaggle_unique_count:,} ({kaggle_unique_pct:.1f}%)")
        
        coverage_summary.append({
            'source': 'Kaggle/External',
            'records': kaggle_enriched,
            'record_pct': kaggle_pct,
            'unique_tracks': kaggle_unique_count,
            'unique_pct': kaggle_unique_pct
        })
        print()
    
    # 2. Spotify API Coverage
    api_file = "data/enriched/spotify_api_enriched_streaming_history.csv"
    if os.path.exists(api_file):
        print("🔗 SPOTIFY API COVERAGE:")
        api_data = pd.read_csv(api_file)
        
        # Check for API metadata
        api_enriched = api_data['spotify_id'].notna().sum()
        api_pct = (api_enriched / total_records) * 100
        
        print(f"   Records with API metadata: {api_enriched:,} ({api_pct:.1f}%)")
        
        # Unique tracks with API metadata
        api_unique = api_data[api_data['spotify_id'].notna()][['master_metadata_track_name', 'master_metadata_album_artist_name']].drop_duplicates()
        api_unique_count = len(api_unique)
        api_unique_pct = (api_unique_count / total_unique_tracks) * 100
        print(f"   Unique tracks with metadata: {api_unique_count:,} ({api_unique_pct:.1f}%)")
        
        coverage_summary.append({
            'source': 'Spotify API',
            'records': api_enriched,
            'record_pct': api_pct,
            'unique_tracks': api_unique_count,
            'unique_pct': api_unique_pct
        })
        print()
    
    # 3. Combined Coverage Estimate
    if len(coverage_summary) > 1:
        print("🔄 COMBINED COVERAGE ESTIMATE:")
        
        # Load both datasets to check overlap
        kaggle_tracks = set()
        api_tracks = set()
        
        if os.path.exists(kaggle_file):
            kaggle_df = pd.read_csv(kaggle_file)
            metadata_cols = ['acousticness', 'danceability', 'energy', 'valence']
            has_kaggle = kaggle_df[metadata_cols].notna().any(axis=1)
            for _, row in kaggle_df[has_kaggle].iterrows():
                kaggle_tracks.add((row['master_metadata_track_name'], row['master_metadata_album_artist_name']))
        
        if os.path.exists(api_file):
            api_df = pd.read_csv(api_file)
            has_api = api_df['spotify_id'].notna()
            for _, row in api_df[has_api].iterrows():
                api_tracks.add((row['master_metadata_track_name'], row['master_metadata_album_artist_name']))
        
        # Calculate overlap and combined coverage
        overlap = len(kaggle_tracks.intersection(api_tracks))
        combined_unique = len(kaggle_tracks.union(api_tracks))
        combined_unique_pct = (combined_unique / total_unique_tracks) * 100
        
        print(f"   Overlap between sources: {overlap:,} tracks")
        print(f"   Combined unique coverage: {combined_unique:,} tracks ({combined_unique_pct:.1f}%)")
        
        # Estimate combined record coverage (approximate)
        total_enriched_records = sum([item['records'] for item in coverage_summary])
        # Rough adjustment for overlap (conservative estimate)
        estimated_combined = total_enriched_records * 0.9  # Assume ~10% overlap in records
        estimated_pct = (estimated_combined / total_records) * 100
        print(f"   Estimated combined record coverage: {estimated_combined:,.0f} ({estimated_pct:.1f}%)")
        print()
    
    # 4. Remaining Work
    print("⏳ REMAINING WORK:")
    
    # Check API progress file
    api_progress_file = "data/enriched/api_metadata_progress.pkl"
    if os.path.exists(api_progress_file):
        import pickle
        with open(api_progress_file, 'rb') as f:
            processed_tracks = pickle.load(f)
        print(f"   API tracks processed so far: {len(processed_tracks):,}")
    
    # Estimate remaining tracks
    if len(coverage_summary) > 0:
        covered_tracks = sum([item['unique_tracks'] for item in coverage_summary])
        remaining = total_unique_tracks - covered_tracks + overlap  # Add back overlap since we subtracted it twice
        remaining_pct = (remaining / total_unique_tracks) * 100
        print(f"   Unique tracks still needing metadata: ~{remaining:,} ({remaining_pct:.1f}%)")
    
    print()
    
    # 5. Data Quality Summary
    print("📈 AVAILABLE METADATA TYPES:")
    
    if os.path.exists(kaggle_file):
        print("   From Kaggle/External:")
        print("   ✓ Audio features (acousticness, danceability, energy, valence, etc.)")
        print("   ✓ Genre classifications")
        print("   ✓ Track popularity and basic info")
    
    if os.path.exists(api_file):
        print("   From Spotify API:")
        print("   ✓ Official Spotify track/artist IDs and URIs")
        print("   ✓ Track popularity and release dates")
        print("   ✓ Artist popularity and follower counts")
        print("   ✓ Detailed genre information")
        print("   ✓ Album metadata")
    
    print()
    print("=" * 60)
    print("Analysis complete! 🎉")
    print("=" * 60)

if __name__ == "__main__":
    analyze_coverage()
