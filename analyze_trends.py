import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import numpy as np

def load_data():
    """Load the enriched streaming history"""
    try:
        df = pd.read_csv("enriched_streaming_history.csv")
        df["ts"] = pd.to_datetime(df["ts"])
        return df
    except FileNotFoundError:
        print("enriched_streaming_history.csv not found. Run the metadata enrichment script first.")
        return None

def analyze_listening_patterns(df):
    """Analyze basic listening patterns over time"""
    print("\n=== LISTENING PATTERNS ANALYSIS ===")
    
    # Listening by year
    yearly_stats = df.groupby('year').agg({
        'minutes_played': 'sum',
        'master_metadata_track_name': 'count'
    }).round(2)
    yearly_stats.columns = ['Total Hours', 'Total Plays']
    yearly_stats['Total Hours'] = yearly_stats['Total Hours'] / 60
    print("\nListening by Year:")
    print(yearly_stats)
    
    # Listening by hour of day
    hourly_listening = df.groupby('hour')['minutes_played'].sum()
    print(f"\nPeak listening hour: {hourly_listening.idxmax()}:00 ({hourly_listening.max():.1f} total minutes)")
    
    # Listening by weekday
    weekday_listening = df.groupby('weekday')['minutes_played'].sum()
    print(f"\nTop listening day: {weekday_listening.idxmax()} ({weekday_listening.max():.1f} total minutes)")

def analyze_audio_features(df):
    """Analyze audio features over time"""
    print("\n=== AUDIO FEATURES ANALYSIS ===")
    
    # Filter out tracks without audio features
    df_with_features = df.dropna(subset=['danceability', 'energy', 'valence'])
    
    if len(df_with_features) == 0:
        print("No audio features found. Make sure metadata enrichment was successful.")
        return
    
    print(f"Analyzing {len(df_with_features)} tracks with audio features ({len(df_with_features)/len(df)*100:.1f}% of total)")
    
    # Average audio features by year
    yearly_features = df_with_features.groupby('year')[
        ['danceability', 'energy', 'valence', 'tempo']
    ].mean().round(3)
    
    print("\nAverage Audio Features by Year:")
    print(yearly_features)
    
    # Find your most/least danceable, energetic, and happy periods
    features_analysis = []
    for feature in ['danceability', 'energy', 'valence']:
        yearly_avg = df_with_features.groupby('year')[feature].mean()
        highest_year = yearly_avg.idxmax()
        lowest_year = yearly_avg.idxmin()
        features_analysis.append({
            'Feature': feature.title(),
            'Highest Year': f"{highest_year} ({yearly_avg[highest_year]:.3f})",
            'Lowest Year': f"{lowest_year} ({yearly_avg[lowest_year]:.3f})"
        })
    
    features_df = pd.DataFrame(features_analysis)
    print("\nMusic Mood Trends:")
    print(features_df.to_string(index=False))

def analyze_genres(df):
    """Analyze genre preferences over time"""
    print("\n=== GENRE ANALYSIS ===")
    
    # Filter tracks with genre information
    df_with_genres = df.dropna(subset=['genres'])
    
    if len(df_with_genres) == 0:
        print("No genre information found. Make sure metadata enrichment was successful.")
        return
    
    print(f"Analyzing {len(df_with_genres)} tracks with genre info ({len(df_with_genres)/len(df)*100:.1f}% of total)")
    
    # Split genres and count
    all_genres = []
    for genres_str in df_with_genres['genres']:
        if pd.notna(genres_str):
            genres = [g.strip() for g in genres_str.split(',')]
            all_genres.extend(genres)
    
    genre_counts = pd.Series(all_genres).value_counts()
    print(f"\nTop 10 Genres by Play Count:")
    print(genre_counts.head(10))
    
    # Genre evolution over time (for top 5 genres)
    top_genres = genre_counts.head(5).index
    
    genre_by_year = {}
    for year in df_with_genres['year'].unique():
        year_data = df_with_genres[df_with_genres['year'] == year]
        year_genres = []
        for genres_str in year_data['genres']:
            if pd.notna(genres_str):
                genres = [g.strip() for g in genres_str.split(',')]
                year_genres.extend(genres)
        
        year_genre_counts = pd.Series(year_genres).value_counts()
        genre_by_year[year] = year_genre_counts
    
    print(f"\nTop Genres by Year (showing top 5 overall genres):")
    genre_evolution = pd.DataFrame(genre_by_year).fillna(0).T
    for genre in top_genres:
        if genre in genre_evolution.columns:
            print(f"\n{genre}:")
            genre_yearly = genre_evolution[genre].sort_index()
            for year, count in genre_yearly.items():
                if count > 0:
                    print(f"  {year}: {int(count)} plays")

def analyze_top_artists_tracks(df):
    """Analyze most played artists and tracks"""
    print("\n=== TOP ARTISTS & TRACKS ===")
    
    # Top artists by play count
    top_artists = df.groupby('master_metadata_album_artist_name').agg({
        'minutes_played': 'sum',
        'master_metadata_track_name': 'count'
    }).round(2)
    top_artists.columns = ['Total Hours', 'Play Count']
    top_artists['Total Hours'] = top_artists['Total Hours'] / 60
    top_artists = top_artists.sort_values('Total Hours', ascending=False)
    
    print("Top 10 Artists by Listening Time:")
    print(top_artists.head(10))
    
    # Top tracks
    top_tracks = df.groupby(['master_metadata_track_name', 'master_metadata_album_artist_name']).agg({
        'minutes_played': 'sum',
        'master_metadata_track_name': 'count'
    }).round(2)
    top_tracks.columns = ['Total Hours', 'Play Count']
    top_tracks['Total Hours'] = top_tracks['Total Hours'] / 60
    top_tracks = top_tracks.sort_values('Total Hours', ascending=False)
    
    print("\nTop 10 Tracks by Listening Time:")
    for (track, artist), row in top_tracks.head(10).iterrows():
        print(f"{track} by {artist}: {row['Total Hours']:.1f} hours ({int(row['Play Count'])} plays)")

def create_visualizations(df):
    """Create some basic visualizations"""
    print("\n=== CREATING VISUALIZATIONS ===")
    
    plt.style.use('seaborn-v0_8')
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle('Your Spotify Listening Analysis', fontsize=16, fontweight='bold')
    
    # 1. Listening hours by year
    yearly_hours = df.groupby('year')['minutes_played'].sum() / 60
    axes[0, 0].bar(yearly_hours.index, yearly_hours.values)
    axes[0, 0].set_title('Total Listening Hours by Year')
    axes[0, 0].set_xlabel('Year')
    axes[0, 0].set_ylabel('Hours')
    axes[0, 0].tick_params(axis='x', rotation=45)
    
    # 2. Listening by hour of day
    hourly_listening = df.groupby('hour')['minutes_played'].sum()
    axes[0, 1].plot(hourly_listening.index, hourly_listening.values, marker='o')
    axes[0, 1].set_title('Listening Patterns by Hour of Day')
    axes[0, 1].set_xlabel('Hour')
    axes[0, 1].set_ylabel('Total Minutes')
    axes[0, 1].set_xticks(range(0, 24, 4))
    
    # 3. Audio features over time (if available)
    df_with_features = df.dropna(subset=['danceability', 'energy', 'valence'])
    if len(df_with_features) > 0:
        yearly_features = df_with_features.groupby('year')[['danceability', 'energy', 'valence']].mean()
        for feature in ['danceability', 'energy', 'valence']:
            axes[1, 0].plot(yearly_features.index, yearly_features[feature], marker='o', label=feature.title())
        axes[1, 0].set_title('Audio Features Trends Over Time')
        axes[1, 0].set_xlabel('Year')
        axes[1, 0].set_ylabel('Feature Value (0-1)')
        axes[1, 0].legend()
        axes[1, 0].tick_params(axis='x', rotation=45)
    else:
        axes[1, 0].text(0.5, 0.5, 'No audio features data\navailable', 
                       ha='center', va='center', transform=axes[1, 0].transAxes)
        axes[1, 0].set_title('Audio Features Trends Over Time')
    
    # 4. Top artists
    top_artists = df.groupby('master_metadata_album_artist_name')['minutes_played'].sum().sort_values(ascending=True).tail(10)
    axes[1, 1].barh(range(len(top_artists)), top_artists.values / 60)
    axes[1, 1].set_yticks(range(len(top_artists)))
    axes[1, 1].set_yticklabels([artist[:20] + '...' if len(artist) > 20 else artist for artist in top_artists.index])
    axes[1, 1].set_title('Top 10 Artists by Total Hours')
    axes[1, 1].set_xlabel('Hours')
    
    plt.tight_layout()
    plt.savefig('spotify_analysis.png', dpi=300, bbox_inches='tight')
    print("Saved visualization to spotify_analysis.png")
    plt.show()

def main():
    """Main analysis function"""
    df = load_data()
    if df is None:
        return
    
    print(f"Loaded {len(df)} streaming records from {df['year'].min()} to {df['year'].max()}")
    print(f"Total listening time: {df['minutes_played'].sum() / 60:.1f} hours")
    print(f"Unique tracks: {df['master_metadata_track_name'].nunique()}")
    print(f"Unique artists: {df['master_metadata_album_artist_name'].nunique()}")
    
    # Run all analyses
    analyze_listening_patterns(df)
    analyze_audio_features(df)
    analyze_genres(df)
    analyze_top_artists_tracks(df)
    create_visualizations(df)

if __name__ == "__main__":
    main()
