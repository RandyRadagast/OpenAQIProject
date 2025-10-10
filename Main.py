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

#URLs
measurementsURL = "https://api.openaq.org/v3/measurements"
headers = {'X-API-Key': API_KEY}
locationsURL = "https://api.openaq.org/v3/locations"


#config params
parameters = ["pm25", "pm10", "no2", "o3"]
date_from = "2025-09-01T00:00:00Z"
date_to   = "2025-09-07T23:59:59Z"
limit = 200
MaximumRetries = 5
WaitTime = 2
print('Parameters set')

#check raw directory exists.
RawDir = Path("data/raw")
RawDir.mkdir(parents=True, exist_ok=True)

bbox = "42.7,-73.5,47.5,-66.9"

print("Fetching New England locations...")

locations = []
page = 1

while True:
    params = {'coordinates': bbox, 'limit': limit, 'page': page}
    try:
        response = requests.get(locationsURL, params=params, headers=headers)
        response.raise_for_status()
        results = response.json().get('results')
        if not results:
            break
        locations.extend(results)
        print('Fetched {} locations'.format(len(results)))
        page += 1
    except requests.exceptions.RequestException as e:
        print(f'Request failed for locations page {page}: {e}')
        time.sleep(WaitTime)
        continue


sensor_ids = []
for loc in locations:
    for sensor in loc.get("sensors", []):
        sensor_ids.append(sensor["id"])
print('Fetched {} sensors'.format(len(sensor_ids)))


for sensor_id in sensor_ids:
    print('Fetching measurements for {}'.format(sensor_id))
    page = 1
    while True:
        params = {
            "sensor_id": sensor_id,
            "parameter": ",".join(parameters),
            "date_from": date_from,
            "date_to": date_to,
            "limit": limit,
            "page": page
        }
        retry = 0
        success = False
        while retry < MaximumRetries and not success:
            try:
                response = requests.get(measurementsURL, params=params, headers=headers)
                if response.status_code == 404:
                    success = True
                    break
                response.raise_for_status()
                results = response.json().get('results')
                if not results:
                    success = True
                    break

                filePath = RawDir.joinpath(f"Measurements_Sensor_{sensor_id}.json")
                SaveJSON(filePath, results)
                print('Measurements saved to {}'.format(filePath))
                page += 1
                success = True
            except requests.exceptions.RequestException as e:
                print(f'Request failed for measurements page {page}: {e}')
                retry += 1
                wait = retry * WaitTime
                time.sleep(wait)
        if not success or not results:
            break


print("US data fetch complete.")






#US: 155
