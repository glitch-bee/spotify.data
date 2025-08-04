import pandas as pd

INPUT_FILE = "combined_streaming_history.csv"
OUTPUT_FILE = "cleaned_streaming_history.csv"

# Load the combined CSV
df = pd.read_csv(INPUT_FILE)

# Convert timestamp to datetime
df["ts"] = pd.to_datetime(df["ts"], utc=True)

# Extract useful time-based fields
df["year"] = df["ts"].dt.year
df["month"] = df["ts"].dt.month
df["day"] = df["ts"].dt.day
df["weekday"] = df["ts"].dt.day_name()
df["hour"] = df["ts"].dt.hour

# Convert ms_played to minutes
df["minutes_played"] = df["ms_played"] / 60000

# Optional: Filter out skipped or very short plays (< 30 seconds)
df = df[df["ms_played"] >= 30000]

# Save cleaned data
df.to_csv(OUTPUT_FILE, index=False)

print(f"Cleaned data saved to {OUTPUT_FILE}")
print(f"Remaining records: {len(df)}")
