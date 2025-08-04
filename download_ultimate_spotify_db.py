import kagglehub
import pandas as pd
import os

# Download latest version
print("Downloading Ultimate Spotify Tracks DB...")
path = kagglehub.dataset_download("zaheenhamidani/ultimate-spotify-tracks-db")
print("Path to dataset files:", path)

# List files in the dataset
print("\nFiles in dataset:")
for file in os.listdir(path):
    print(f"  {file}")
    file_path = os.path.join(path, file)
    if os.path.isfile(file_path):
        size_mb = os.path.getsize(file_path) / (1024 * 1024)
        print(f"    Size: {size_mb:.1f} MB")

# Load and examine the data
csv_files = [f for f in os.listdir(path) if f.endswith('.csv')]
if csv_files:
    for csv_file in csv_files:
        print(f"\n=== Examining {csv_file} ===")
        df = pd.read_csv(os.path.join(path, csv_file))
        print(f"Shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        print(f"\nFirst few rows:")
        print(df.head())
        
        # Check for track/artist columns
        track_cols = [col for col in df.columns if 'track' in col.lower() or 'name' in col.lower()]
        artist_cols = [col for col in df.columns if 'artist' in col.lower()]
        print(f"\nPotential track columns: {track_cols}")
        print(f"Potential artist columns: {artist_cols}")
        
        # Show unique values for key columns
        if 'genre' in str(df.columns).lower():
            genre_cols = [col for col in df.columns if 'genre' in col.lower()]
            for genre_col in genre_cols:
                print(f"\n{genre_col} sample values:")
                print(df[genre_col].value_counts().head(10))
