import os
import pandas as pd
import requests
from pathlib import Path
import io  # Import io to handle StringIO

# Base URL for raw GitHub content
base_url = "https://raw.githubusercontent.com/DiogoFerrari/replication-twitter-deplatforming/refs/heads/master/"

# Function to download and read CSV files from GitHub
def download_csv(file_path):
    url = base_url + file_path
    print(f"Requesting URL: {url}")  # Debugging line to print the URL
    response = requests.get(url)
    if response.status_code == 200:
        try:
            return pd.read_csv(io.StringIO(response.text))  # Use io.StringIO to handle response text
        except Exception as e:
            print(f"Error reading CSV from {file_path}: {e}")
            return None
    else:
        print(f"Failed to download {file_path} with status code {response.status_code}")
        return None

# Identify data folders and files with their respective part ranges
data_folders = {
    "data/final/decahose-daily-totals.csv": 61,  # 00.part to 60.part
    "data/final/panel-2016-daily-totals.csv": 61,  # 00.part to 60.part
    "data/final/panel-2020-daily-totals.csv": 122  # 000.part to 121.part
}

all_dfs = []

# Process each data folder
for folder, num_parts in data_folders.items():
    try:
        # Generate part file paths based on the number of parts
        if num_parts == 122:  # Handle 3-digit parts for panel-2020-daily-totals.csv
            part_files = [f"{folder}/{str(f).zfill(3)}.part" for f in range(num_parts)]
        else:  # Handle 2-digit parts for other folders
            part_files = [f"{folder}/{str(f).zfill(2)}.part" for f in range(num_parts)]

        for file_path in part_files:
            df = download_csv(file_path)
            if df is not None:
                all_dfs.append(df)
                print(f"Successfully loaded {file_path}")

    except Exception as e:
        print(f"Error processing {folder}: {e}")

if all_dfs:
    combined_df = pd.concat(all_dfs, ignore_index=True)

    # Ensure the dataframe has all required columns
    required_columns = [
        'date', 'fake_merged', 'fake_merged_initiation', 'fake_merged_rt',
        'fake_grinberg_initiation', 'fake_grinberg_rt', 'fake_grinberg_rb_initiation',
        'fake_grinberg_rb_rt', 'fake_newsguard_initiation', 'fake_newsguard_rt',
        'not_fake_shopping', 'not_fake_shopping_initiation', 'not_fake_shopping_rt',
        'not_fake_sports', 'not_fake_sports_initiation', 'not_fake_sports_rt',
        'n', 'stat', 'nusers', 'group', 'post_treatment', 'treatment_group'
    ]

    # Check for missing columns
    missing_columns = [col for col in required_columns if col not in combined_df.columns]
    if missing_columns:
        print(f"Warning: Missing columns: {missing_columns}")
    
    # Save the combined data
    combined_df.to_csv("mccabe_public_data.csv", index=False)
    print("Successfully created mccabe_public_data.csv")
else:
    print("No data files were successfully loaded")
   