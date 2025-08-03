import spotipy
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")

# Set up auth
auth_manager = SpotifyOAuth(
    client_id=CLIENT_ID, 
    client_secret=CLIENT_SECRET,
    redirect_uri="http://127.0.0.1:8888/callback",
    scope="user-library-read",
    cache_path=".spotify_cache"
)
sp = spotipy.Spotify(auth_manager=auth_manager)

print("Testing track ID retrieval and comparison...")

# Test with a few different tracks
test_queries = [
    ("Losing Keys", "Jack Johnson"),
    ("Strobe", "deadmau5"),
    ("Some Chords", "deadmau5"),
    ("Midnight City", "M83")
]

for track_name, artist_name in test_queries:
    print(f"\n=== Testing: '{track_name}' by {artist_name} ===")
    
    # Search for track
    query = f"track:{track_name} artist:{artist_name}"
    try:
        results = sp.search(q=query, type="track", limit=3)  # Get top 3 matches
        items = results["tracks"]["items"]
        
        if items:
            for i, track in enumerate(items):
                print(f"Match {i+1}:")
                print(f"  Name: {track['name']}")
                print(f"  Artist: {track['artists'][0]['name']}")
                print(f"  Album: {track['album']['name']}")
                print(f"  Track ID: {track['id']}")
                print(f"  Popularity: {track['popularity']}")
                print(f"  Release Date: {track['album']['release_date']}")
                
                # Test audio features for each ID
                try:
                    features = sp.audio_features([track['id']])
                    if features and features[0]:
                        print(f"  ✓ Audio features work")
                    else:
                        print(f"  ✗ Audio features returned None")
                except Exception as e:
                    print(f"  ✗ Audio features failed: {e}")
        else:
            print("  No matches found")
            
    except Exception as e:
        print(f"  Search failed: {e}")

print(f"\n=== Testing direct track ID access ===")
# Test with a known good track ID (Spotify's test track)
test_track_id = "4uLU6hMCjMI75M1A2tKUQC"  # Never Gonna Give You Up - Rick Astley
print(f"Testing with known track ID: {test_track_id}")

try:
    track_info = sp.track(test_track_id)
    print(f"✓ Track info: {track_info['name']} by {track_info['artists'][0]['name']}")
    
    features = sp.audio_features([test_track_id])
    if features and features[0]:
        print(f"✓ Audio features work: danceability = {features[0]['danceability']}")
    else:
        print(f"✗ Audio features returned None")
        
except Exception as e:
    print(f"✗ Direct track access failed: {e}")

# Check if the issue is with your specific account/app
print(f"\n=== Account/App Status ===")
try:
    user = sp.current_user()
    print(f"User: {user['display_name']} ({user['id']})")
    print(f"Country: {user.get('country', 'Unknown')}")
    print(f"Product: {user.get('product', 'Unknown')}")
except Exception as e:
    print(f"User info failed: {e}")
