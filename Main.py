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
measurementsURL = "https://api.openaq.org/v3/measurements"
headers = {'X-API-Key': API_KEY}
locationsURL = "https://api.openaq.org/v3/locations"


#config params
parameters = ["pm25", "pm10", "no2", "o3"]
date_from = "2025-09-01T00:00:00Z"
date_to   = "2025-09-07T23:59:59Z"
limit = 50
MaximumRetries = 5
WaitTime = 1
print('Parameters set')

#check/create raw directory.
RawDir = Path("data/raw")
RawDir.mkdir(parents=True, exist_ok=True)

bbox = "42.7,-73.5,47.5,-66.9"

print("Fetching New England locations...")

locations = []
page = 1
params = {
    "coordinates": bbox,
    "limit": 1000,  # fetch as many as possible per page
    "page": 1
}
response = requests.get(locationsURL, headers=headers, params=params, timeout=30)
response.raise_for_status()
results = response.json()

locations = results.get("results", [])
page += 1

print(f"Found {len(locations)} locations in New England.")

sensor_ids = []
for loc in locations:
    for sensor in loc.get("sensors", []):
        sensor_ids.append(sensor["id"])

print(f"Prepared {len(sensorFetch)} sensors to fetch data from.")

for sensor_id in sensor_ids:
    page = 1
    while True:
        params = {
            "sensor_id": sensor_id,
            "parameter": ",".join(parameters),
            "date_from": "2023-01-01T00:00:00Z",
            "date_to": "2023-01-07T23:59:59Z",
            "limit": 200,
            "page": page
        }
        resp = requests.get(measurementsURL, headers=headers, params=params)
        if resp.status_code == 404:
            break
        resp.raise_for_status()
        results = resp.json().get("results", [])
        if not results:
            break

        # Save or process results here
        print(f"Sensor {sensor_id} page {page} returned {len(results)} measurements")
        page += 1

print("US data fetch complete.")






#US: 155
