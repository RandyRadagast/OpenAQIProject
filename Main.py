from pathlib import Path
import requests
import json
import time
import os
import pandas as pd
from dotenv import load_dotenv


#function to save each JSON file.
def SaveJSON(file_path, data, append=True):
    print('Saving RAW JSON')
    mode = 'a' if append else 'w'
    with open(file_path, mode, encoding='utf-8') as f:
        for rec in data:
            f.write(json.dumps(rec, ensure_ascii=False) + '\n')
    print('Saved RAW JSON')

#retreive API Key
print('Fetching key')
load_dotenv()
API_KEY = os.getenv("OPENAQ_API_KEY")
if not API_KEY:
    raise ValueError("API_KEY is not set in .env file")
print('Complete')

#URLs to be used for JSON requests
print('URL set')
measurements_URL = "https://api.openaq.org/v3/measurements"
headers = {'X-API-Key': API_KEY}
locations_URL = "https://api.openaq.org/v3/locations"
all_locations = []


#config params
parameters = ["pm25", "pm10", "no2", "o3"]
date_from = "2023-01-01T00:00:00Z"
date_to   = "2023-01-07T23:59:59Z"
limit = 1000
MaximumRetries = 5
WaitTime = 1
print('Parameters set')

#check/create raw directory.
Path("data/raw").mkdir(parents=True, exist_ok=True)

ll_locations = []
page = 1
while True:
    params = {"country": "US", "limit": limit, "page": page}
    response = requests.get(locations_URL, params=params, headers=headers)
    response.raise_for_status()
    data = response.json()
    results = data.get("results", [])
    if not results:
        break
    all_locations.extend(results)
    print(f"Fetched {len(results)} locations on page {page}")
    page += 1

print(f"Total US locations: {len(all_locations)}")

# Step 2: Fetch measurements for each location
for loc in all_locations:
    loc_id = loc['id']
    page = 1
    while True:
        try:
            params = {
                "location_id": loc_id,
                "parameter": ",".join(parameters),
                "date_from": date_from,
                "date_to": date_to,
                "limit": limit,
                "page": page
            }
            response = requests.get(measurements_URL, params=params, headers=headers, timeout=30)
            if response.status_code == 404:
                print(f"No measurements for location {loc_id}. Skipping.")
                break  # move to next location
            response.raise_for_status()

            results = response.json().get("results", [])
            if not results:
                break

            file_path = f"data/raw/measurements_US_{loc_id}.jsonl"
            SaveJSON(file_path, results)
            print(f"Saved {len(results)} measurements for location {loc_id}, page {page}")
            page += 1

        except requests.exceptions.RequestException as e:
            print(f"Request failed for location {loc_id}: {e}. Retrying in {WaitTime} seconds...")
            time.sleep(WaitTime)
            break  # optional: skip after 1 failure

print("US data fetch complete.")






#US: 155
