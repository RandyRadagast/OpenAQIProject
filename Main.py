from pathlib import Path
import requests
import json
import time
import os
import pandas as pd
from dotenv import load_dotenv
import argparse
import logging

#Setting up logging?
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("data/fetch.log"), #writing to Log file.
        logging.StreamHandler()
    ]
)

#function to save each JSON file.
def SaveJSON(file_path, data, append=True):
    logging.info('Saving RAW JSON')
    mode = 'a' if append else 'w'
    with open(file_path, mode, encoding='utf-8') as f:
        for rec in data:
            f.write(json.dumps(rec, ensure_ascii=False) + '\n')
    logging.info('Saved RAW JSON')



#retreive API Key
logging.info('Fetching key')
load_dotenv()
API_KEY = os.getenv("OPENAQ_API_KEY")
if not API_KEY:
    raise ValueError("API_KEY is not set in .env file")
logging.info('Key retrieved')

#URLs
measurementsURL = "https://api.openaq.org/v3/measurements"
headers = {'X-API-Key': API_KEY}
locationsURL = "https://api.openaq.org/v3/locations"


#config params
parameters = ["pm25", "pm10", "no2", "o3"]
state_bboxes = {
    "ME": "-71.1,43.0,-66.9,47.5",  # Maine
    "NH": "-72.6,42.7,-70.6,45.3",  # New Hampshire
    "VT": "-73.5,42.7,-71.4,45.1",  # Vermont
}
#Configuring ArgParser
parser = argparse.ArgumentParser(description='Fetch Air Quality Data')
parser.add_argument('--start', help='Start Date in ISO Format', default='2025-09-01T00:00:00Z')
parser.add_argument('--end',help='End Date in ISO Format', default='2025-09-07T23:59:59Z')
parser.add_argument('--limit',type=int,help='Records Per Page', default=200)
args = parser.parse_args()
date_from = args.start
date_to = args.end
limit = args.limit
print('start date:', args.start)
print('end date:', args.end)
print('limit:', args.limit)

MaximumRetries = 5
WaitTime = 2
logging.info('Parameters set')

#check raw directory exists.
RawDir = Path("data/raw")
RawDir.mkdir(parents=True, exist_ok=True)

locations = []
allSensors = []

logging.info("Starting data fetch for ME, NH, and VT)")

for state, bbox in state_bboxes.items():
    logging.info(f"Fetching {state} (bbox: {bbox})")
    page = 1

    while True:
        params = {"bbox": bbox, "limit": limit, "page": page}
        try:
            response = requests.get(locationsURL, headers=headers, params=params, timeout=30)

            if response.status_code == 429:
                logging.warning(f"{state} page {page}: rate limit hit, sleeping 10s")
                time.sleep(10)
                continue
            elif response.status_code >= 500:
                logging.error(f"{state} page {page}: server error {response.status_code}, skipping.")
                break

            response.raise_for_status()
            results = response.json().get("results", [])
            if not results:
                logging.info(f"{state}: no more results after page {page-1}")
                break

            #Iterate over each location to pull sensor information
            for loc in results:
                for sensor in loc.get("sensors", []):
                    sensor_info = {
                        "state": state,
                        "location_id": loc.get("id"),
                        "sensor_id": sensor.get("id"),
                        "parameter": sensor.get("parameter"),
                        "sensor_name": loc.get("name"),
                        "coordinates": loc.get("coordinates"),
                    }
                    allSensors.append(sensor_info)

            logging.info(f"{state} page {page}: fetched {len(results)} locations.")
            locations.extend(results)
            page += 1
            time.sleep(WaitTime)

        except requests.exceptions.RequestException as e:
            logging.warning(f"{state} page {page}: request failed ({e}), retrying...")
            time.sleep(WaitTime * 2)
            continue

logging.info(f"Finished fetching. Total locations: {len(locations)}")

for sensor in allSensors:
    sensor_id = sensor["sensor_id"]
    sensor_name = sensor["sensor_name"]
    state = sensor["state"]

    logging.info(f"\nPulling data for {sensor_name} ({state}) sensor {sensor_id}")
    page = 1
    total_measurements = 0

    while True:
        params = {
            "sensor_id": sensor_id,
            "date_from": date_from,
            "date_to": date_to,
            "limit": limit,
            "page": page
        }

        try:
            resp = requests.get(measurementsURL, headers=headers, params=params, timeout=30)
            if resp.status_code == 404:
                logging.warning(f"Sensor {sensor_id} not found.")
                break

            resp.raise_for_status()
            results = resp.json().get("results", [])
            if not results:
                break

            total_measurements += len(results)
            file_path = RawDir / f"{state}_sensor_{sensor_id}.jsonl"
            SaveJSON(file_path, results)
            logging.info(f"Page {page}: saved {len(results)} measurements")

            page += 1
            time.sleep(1)

        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching sensor {sensor_id} page {page}: {e}")
            time.sleep(3)
            break

    logging.info(f"Done — {total_measurements} total measurements")

logging.info("Data fetch complete!")


#Cleaning time.
logging.info('Cleaning Data...')
dfList = []
for file in RawDir.glob("*.json"):
    with open(file, 'r', encoding='utf-8') as f:
        data = [json.loads(line) for line in f]
        dfList.append(pd.DataFrame(data))
df = pd.concat(dfList, ignore_index=True)
df = df.drop_duplicates(subset=['sensor_id', 'date', 'parameter'])#Check for dupes
df['date'] = pd.to_datetime(df['date'], utc=True)
logging.info('Cleaned Data')

logging.info('Saving Data...')
store_path = Path("data/data_store.csv")
if store_path.exists():
    existing_df = pd.read_csv(store_path)
    df = pd.concat([existing_df, df], ignore_index=True)
    df = df.drop_duplicates(subset=['sensor_id', 'date', 'parameter'])#ensure dupes are not saved within file.
df.to_csv(store_path, index=False)
logging.info('Data saved to {}'.format(store_path))



#US: 155
