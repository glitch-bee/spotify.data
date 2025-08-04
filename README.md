# Spotify Data Analysis 

A comprehensive toolkit for analyzing your personal Spotify listening history to discover music trends, habits, and preferences over time.

## Overview

This project processes your extended Spotify streaming history data to:
- Combine multiple JSON history files into a single dataset
- Clean and enrich the data with temporal features
- Enrich metadata using multiple sources (Spotify API + external datasets)
- Analyze listening patterns, genre evolution, and music trends
- Generate visualizations of your musical journey

## Features

- **Data Processing**: Combine and clean multiple Spotify history JSON files
- **Multi-Source Metadata Enrichment**: 
  - Spotify Web API (complete but rate-limited)
  - External Kaggle datasets (fast, high coverage)
  - Automatic matching and merging
- **Comprehensive Analysis**: Audio features, genres, popularity, and trends
- **Organized Structure**: Clean, scalable codebase with proper separation
- **Resume Capability**: All processes can be safely interrupted and resumed

## Project Structure

```
spotify-data/
├── scripts/
│   ├── data_processing/
│   │   ├── clean-history.py           # Clean and enhance streaming data
│   │   └── combine-history.py         # Combine multiple JSON files
│   ├── external_matching/
│   │   └── ultimate_spotify_matcher.py  # Match against Kaggle datasets
│   └── spotify_api/
│       └── metadata-basic-enrichment.py  # Spotify API metadata collection
├── data/
│   ├── raw/                           # Original JSON files from Spotify
│   ├── processed/                     # Cleaned and combined CSV files
│   └── enriched/                      # Metadata-enriched datasets
├── external_datasets/                 # Downloaded Kaggle datasets
├── docs/                             # Original Spotify data files
└── requirements.txt                  # Python dependencies
```

## Current Status

### Data Coverage (Last Updated)
- **Raw Listening Data**: 137,854 total plays (15 years of history)
- **External Dataset Enrichment**: 30.6% coverage (42,473 enriched records)
  - Ultimate Spotify DB: 22.4% unique track matches (6,130 tracks)
  - Complete audio features and genre data
- **Spotify API Enrichment**: 18% complete (paused due to rate limits)
  - Will auto-resume when rate limit resets

### Available Metadata
- **Audio Features**: Danceability, energy, valence, tempo, acousticness, instrumentalness, liveness, loudness, speechiness
- **Track Info**: Release dates, popularity scores, explicit content flags
- **Genre Data**: Detailed genre classifications with play counts
- **Artist Info**: Complete artist metadata and classifications

## Setup

### 1. Get Your Spotify Data

1. Request your extended streaming history from [Spotify Privacy Settings](https://www.spotify.com/account/privacy/)
2. Wait for the email with your data (can take up to 30 days)
3. Extract the JSON files to a `docs/` folder in this project

### 2. Set Up Python Environment

```bash
# Create a virtual environment (recommended)
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install required packages
pip install pandas spotipy tqdm matplotlib seaborn python-dotenv kagglehub
```

### 3. Set Up Spotify API Credentials (Optional)

While external datasets provide good coverage, you may want API access for additional data:

1. Go to [Spotify Developer Dashboard](https://developer.spotify.com/dashboard)
2. Create a new app to get your Client ID and Client Secret
3. Copy `.env.example` to `.env`:
   ```bash
   cp .env.example .env
   ```
4. Fill in your credentials:
   ```env
   SPOTIPY_CLIENT_ID=your_client_id_here
   SPOTIPY_CLIENT_SECRET=your_client_secret_here
   SPOTIPY_REDIRECT_URI=http://localhost:8888/callback
   ```

## Usage

### Quick Start (Recommended)

For the fastest results with good metadata coverage:

1. **Process Your Raw Data**
   ```bash
   python scripts/data_processing/combine-history.py
   python scripts/data_processing/clean-history.py
   ```

2. **Enrich with External Datasets** (Fast, no API limits)
   ```bash
   python scripts/external_matching/ultimate_spotify_matcher.py
   ```
   This provides ~30% coverage with complete audio features and genres.

### Advanced Usage

**For Maximum Coverage**: Combine external datasets with Spotify API enrichment:

1. Set up your `.env` file with Spotify API credentials
2. Run external matching first (above)
3. Run API enrichment:
   ```bash
   python scripts/spotify_api/metadata-basic-enrichment.py
   ```

### Data Processing Steps

#### Step 1: Combine History Files
```bash
python scripts/data_processing/combine-history.py
```
- Combines all JSON files from `docs/` folder
- Creates `data/processed/combined_streaming_history.csv`

#### Step 2: Clean and Enhance Data
```bash
python scripts/data_processing/clean-history.py
```
- Creates `data/processed/cleaned_streaming_history.csv` with:
  - Converted timestamps and time-based features
  - Filtered short plays (<30 seconds) 
  - Play duration in minutes
  - Weekday, hour, and temporal analysis features

#### Step 3: Metadata Enrichment

**Option A: External Datasets (Recommended First)**
```bash
python scripts/external_matching/ultimate_spotify_matcher.py
```
- Downloads Ultimate Spotify DB from Kaggle
- Matches tracks using fuzzy string matching
- Provides audio features, genres, popularity data
- No rate limits, fast processing

**Option B: Spotify Web API**
```bash
python scripts/spotify_api/metadata-basic-enrichment.py
```
- Requires Spotify API credentials
- More comprehensive but rate-limited
- Automatically resumes from progress saves
- Best used after external matching for remaining tracks
- Processes all tracks in one go
- Faster but more aggressive

Both create `enriched_streaming_history.csv` with additional metadata:
- Spotify track IDs
## Data Analysis

### Available Data Files

After processing, you'll have these datasets:

- `data/processed/cleaned_streaming_history.csv` - Cleaned listening history with temporal features
- `data/enriched/ultimate_spotify_enriched_*.csv` - External dataset enrichment results  
- `data/enriched/spotify_api_enriched_*.csv` - API enrichment results (when available)

### What You Can Analyze

**Temporal Patterns**:
- Listening habits by time of day, day of week, season
- Evolution of music taste over 15 years
- Monthly and yearly listening volume trends

**Audio Feature Analysis**:
- Preference for danceability, energy, valence over time
- Tempo preferences and changes
- Acousticness vs. electronic music trends

**Genre Evolution**:
- Top genres by play count and listening time
- Genre diversity and discovery patterns
- Seasonal genre preferences

**Discovery Patterns**:
- Track popularity vs. personal preference
- Artist loyalty and discovery rates
- Repeat listening behavior

## Generated Files Overview

## Generated Files Overview

The project creates organized output files:

```
data/
├── processed/
│   ├── combined_streaming_history.csv    # Raw combined data
│   └── cleaned_streaming_history.csv     # Processed with time features
├── enriched/
│   ├── ultimate_spotify_enriched_*.csv   # External dataset results
│   └── spotify_api_enriched_*.csv        # API enrichment results
└── external_datasets/
    └── ultimate_spotify_db/               # Downloaded Kaggle datasets
```

## Available Analysis Features

With enriched data, you can analyze:

### Listening Patterns
- Total hours and play counts by year, month, season
- Peak listening hours and days of week
- Seasonal and temporal trends across 15 years

### Audio Features Evolution  
- Musical taste evolution over time
- Trends in danceability, energy, valence, tempo
- Acoustic vs. electronic preferences
- Mood patterns (valence/energy correlations)

### Genre Analysis
- Top genres by play count and listening time
- Genre discovery and evolution patterns
- Seasonal genre preferences
- Musical diversity metrics

### Discovery & Popularity
- Track popularity vs. personal preference
- Artist loyalty and discovery rates
- Repeat listening behavior analysis
- Mainstream vs. niche music preferences

## Technical Notes

### API Rate Limiting
The Spotify API enrichment includes:
- Respectful rate limiting (0.2s between requests)
- Progress saving for interruption/resumption
- Comprehensive error handling
- Automatic retry logic

### External Dataset Benefits
- **No Rate Limits**: Process entire dataset quickly
- **High Coverage**: 30%+ match rates with quality data
- **Complete Features**: All audio features and genres included
- **Reliable**: No authentication or quota concerns

### Data Quality
- Fuzzy string matching for robust track identification
- Duplicate detection and handling
- Comprehensive metadata validation
- Multiple source integration for maximum coverage

## Data Privacy & Security

- Your `.env` file with API credentials is excluded from git
- All CSV files with personal listening data are gitignored
- External datasets contain only public metadata
- Never commit or share personal listening data

## Contributing

This project welcomes contributions! Areas for improvement:
- Additional external dataset integrations
- Advanced analysis and visualization scripts
- Performance optimizations
- New matching algorithms

## Troubleshooting

### Common Issues
1. **API Rate Limits**: Use external datasets first, then supplement with API
2. **Low Match Rates**: Check for special characters in track/artist names
3. **Missing Data**: Some tracks may not exist in any dataset
4. **Memory Issues**: Process in smaller batches if needed

### Getting Help
- Check the progress files for API enrichment status
- Review console output for match statistics
- Verify your raw data format matches expected structure

## License

This project is for personal use. Respect Spotify's API terms of service and your data privacy.
