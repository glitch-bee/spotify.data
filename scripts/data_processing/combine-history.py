import os
import json
import pandas as pd

# Set the folder where your JSON files are
DATA_FOLDER = './docs'
OUTPUT_FILE = 'combined_streaming_history.csv'

# Collect all matching JSON files
json_files = sorted([
    f for f in os.listdir(DATA_FOLDER)
    if f.startswith('Streaming_History') and f.endswith('.json')
])

combined_data = []

# Load and combine all JSON files
for file in json_files:
    path = os.path.join(DATA_FOLDER, file)
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        combined_data.extend(data)

# Create a DataFrame
df = pd.DataFrame(combined_data)

# Save to CSV
df.to_csv(OUTPUT_FILE, index=False)

print(f"Combined {len(json_files)} files into {OUTPUT_FILE}")
print(f"Total records: {len(df)}")
