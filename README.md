# Spotify Data Analysis 🎵

A comprehensive toolkit for analyzing your personal Spotify listening history to discover music trends, habits, and preferences over time.

## Overview

This project processes your extended Spotify streaming history data to:
- Combine multiple JSON history files into a single dataset
- Clean and enrich the data with temporal features
- Fetch additional metadata from Spotify's API (genres, audio features)
- Analyze listening patterns, genre evolution, and music trends
- Generate visualizations of your musical journey

## Features

- **Data Processing**: Combine and clean multiple Spotify history JSON files
- **Metadata Enrichment**: Fetch audio features (danceability, energy, valence, etc.) and genres
- **Trend Analysis**: Discover patterns in your listening habits over time
- **Visualizations**: Generate charts showing your music evolution
- **Batched Processing**: Efficient, rate-limited API calls with resume capability

## Setup

### 1. Get Your Spotify Data

1. Request your extended streaming history from [Spotify Privacy Settings](https://www.spotify.com/account/privacy/)
2. Wait for the email with your data (can take up to 30 days)
3. Extract the JSON files to a `docs/` folder in this project

### 2. Set Up Spotify API Credentials

1. Go to [Spotify Developer Dashboard](https://developer.spotify.com/dashboard)
2. Create a new app to get your Client ID and Client Secret
3. Copy `.env.example` to `.env`:
   ```bash
   cp .env.example .env
   ```
4. Add your credentials to `.env`:
   ```
   SPOTIFY_CLIENT_ID=your_client_id_here
   SPOTIFY_CLIENT_SECRET=your_client_secret_here
   ```

### 3. Install Dependencies

```bash
# Create a virtual environment (recommended)
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install required packages
pip install pandas spotipy tqdm matplotlib seaborn python-dotenv
```

## Usage

### Step 1: Combine Your History Files
```bash
python combine-history.py
```
This creates `combined_streaming_history.csv` from all JSON files in the `docs/` folder.

### Step 2: Clean the Data
```bash
python clean-history.py
```
This creates `cleaned_streaming_history.csv` with:
- Converted timestamps
- Time-based features (year, month, hour, weekday)
- Filtered out very short plays (<30 seconds)
- Play duration in minutes

### Step 3: Enrich with Metadata (Choose One)

**Option A: Batched Processing (Recommended)**
```bash
python metadata-history-batched.py
```
- Processes tracks in batches of 100
- Saves progress between batches
- Can be safely stopped and resumed
- More respectful to Spotify's API

**Option B: Original Script**
```bash
python metadata-history-cleaned.py
```
- Processes all tracks in one go
- Faster but more aggressive

Both create `enriched_streaming_history.csv` with additional metadata:
- Spotify track IDs
- Audio features (danceability, energy, valence, tempo, etc.)
- Artist genres

### Step 4: Analyze Your Trends
```bash
python analyze_trends.py
```
This generates:
- Console output with statistics and trends
- `spotify_analysis.png` with visualizations

## File Structure

```
spotify-data/
├── docs/                               # Your Spotify JSON files
│   ├── Streaming_History_Audio_*.json
│   └── Streaming_History_Video_*.json
├── combine-history.py                  # Combine JSON files
├── clean-history.py                    # Clean and process data
├── metadata-history-cleaned.py         # Original metadata script
├── metadata-history-batched.py         # Improved batched script
├── analyze_trends.py                   # Analysis and visualization
├── .env                               # Your API credentials (not in git)
├── .env.example                       # Template for credentials
└── README.md                          # This file
```

## Generated Files

- `combined_streaming_history.csv` - Raw combined data
- `cleaned_streaming_history.csv` - Processed data with time features
- `enriched_streaming_history.csv` - Final dataset with Spotify metadata
- `metadata_progress.pkl` - Progress file for batched processing
- `spotify_analysis.png` - Visualization charts

## Analysis Features

The analysis script provides insights into:

### Listening Patterns
- Total hours and play counts by year
- Peak listening hours and days
- Seasonal trends

### Audio Features Evolution
- How your music taste changed over time
- Trends in danceability, energy, and valence
- Tempo preferences

### Genre Analysis
- Top genres by play count
- Genre evolution over the years
- Discovering your musical journey

### Top Content
- Most played artists and tracks
- Listening time breakdown
- Play count statistics

## Rate Limiting & API Usage

The batched script is designed to be respectful to Spotify's API:
- 0.2 seconds between individual requests
- 5 seconds between batches of 100 tracks
- Progress saving allows safe interruption and resumption
- Comprehensive error handling and logging

## Data Privacy

- Your `.env` file with API credentials is excluded from git
- CSV files with your listening data are excluded from git
- Only share the code, never your personal data or credentials

## Contributing

Feel free to open issues or submit pull requests with improvements!

## License

This project is for personal use. Respect Spotify's API terms of service and your data privacy.
