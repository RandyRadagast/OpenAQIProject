from pathlib import Path
import requests
import json
import time
import os
import pandas as pd
from dotenv import load_dotenv
import argparse
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

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

def Retrieval(d, *keys):
    for k in keys:
        d = d.get(k, {}) if isinstance(d, dict) else None
    return d


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
    # "NH": "-72.6,42.7,-70.6,45.3",  # New Hampshire
    # "VT": "-73.5,42.7,-71.4,45.1",  # Vermont
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

#check the cleaned directory exists, create if it doesn't
CleanDir = Path("data/clean")
CleanDir.mkdir(parents=True, exist_ok=True)

locations = []
allSensors = []

logging.info("Starting data fetch for ME)")
for state, bbox in state_bboxes.items():
    logging.info(f"Fetching {state} (bbox: {bbox})")
    page = 1
    retry = 0

    while True:
        params = {"bbox": bbox, "limit": limit, "page": page}
        try:
            retry+=1
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

            #Store avail sensors
            locations.extend(results)
            page += 1
            time.sleep(WaitTime)

        except requests.exceptions.RequestException as e:
            if retry >= MaximumRetries:
                logging.error(f"{state} page {page}: Request Failed {e}, Maximum attempts reached: Skipping.")
                break
            else:
                retry+=1
                logging.warning(f"{state} page {page}: request failed ({e}), retrying...")
                time.sleep(WaitTime * retry)
                continue

logging.info(f"Finished fetching. Total locations: {len(locations)}")

for sensor in allSensors:
    sensor_id = sensor["sensor_id"]
    sensor_name = sensor["sensor_name"]
    state = sensor["state"]

    logging.info(f"Pulling data for {sensor_name} ({state}) sensor {sensor_id}")
    page = 1
    total_measurements = 0
    retry = 0
    WaitTime = 2
    today = datetime.now(ZoneInfo("UTC")).strftime("%Y-%m-%d")

    while True:
        params = {
            "date_from": date_from,
            "date_to": date_to,
            "limit": limit,
            "page": page
        }
        URL = f"https://api.openaq.org/v3/sensors/{sensor_id}/days"
        try:
            resp = requests.get(URL, headers=headers, params=params, timeout=30)
            if resp.status_code == 404:
                logging.warning(f"Sensor {sensor_id} not found.")
                break

            resp.raise_for_status()
            results = resp.json().get("results", [])
            if not results:
                logging.info(f"{sensor_id}: no more results after page {page - 1}")
                break

            total_measurements += len(results)
            RawFilePath = RawDir / f"{today}.jsonl"
            SaveJSON(RawFilePath, results)
            logging.info(f"Page {page}: saved {len(results)} measurements to {RawFilePath}")

            page += 1
            time.sleep(1)

        except requests.exceptions.RequestException as e:
            if retry >= MaximumRetries:
                logging.error(f'Error fetching sensor {sensor_id} page {page}: Request Failed {e}, Maximum attempts reached. Moving on.')
                break
            else:
                retry+=1
                logging.error(f"Error fetching sensor {sensor_id} page {page}: {e} retrying...")
                time.sleep(WaitTime * retry)
            continue

    logging.info(f"Done — {total_measurements} total measurements")
logging.info("Data fetch complete!")


#Cleaning time.


#Defining file just created
NewFile = RawDir / f'{today}.jsonl'
if not NewFile.exists():
    logging.error('File not found, exiting.')
    raise SystemExit

logging.info('Cleaning Data...')
with open(NewFile) as f:
    records = [json.loads(line) for line in f if line.strip()]

if not records:
    logging.warning(f"{NewFile.name}: file is empty or unreadable.")
    raise SystemExit(1)

dFrame = pd.DataFrame(records)
logging.info(f'Loaded {len(records)} records')

dFrame = dFrame[dFrame["parameter"].apply(lambda x: isinstance(x, dict) and x.get("name") in ["pm25", "o3"])]
logging.info(f'Filtering down to {len(dFrame)} records')

#verifying format
dFrame["datetimeFrom"] = dFrame["period"].apply(lambda x: Retrieval(x, "datetimeFrom", "utc"))
dFrame["datetimeTo"]   = dFrame["period"].apply(lambda x: Retrieval(x, "datetimeTo", "utc"))
dFrame["avg_value"]    = dFrame["summary"].apply(lambda x: x.get("avg") if isinstance(x, dict) else None)

dFrame["parameter"] = dFrame["parameter"].apply(lambda x: x.get("name") if isinstance(x, dict) else x)

dFrame["avg_value"] = pd.to_numeric(dFrame["avg_value"], errors="coerce")
dFrame["datetimeFrom"] = pd.to_datetime(dFrame["datetimeFrom"], utc=True, errors="coerce")
dFrame["datetimeTo"] = pd.to_datetime(dFrame["datetimeTo"], utc=True, errors="coerce")

# Drop incomplete rows
before = len(dFrame)
df = dFrame.dropna(subset=["avg_value", "datetimeFrom"])
after = len(df)
logging.info(f"Dropped {before - after} invalid or incomplete rows.")

#Dedupe - such a fun word.
df = df.drop_duplicates(subset=["parameter", "datetimeFrom"])
logging.info(f"After deduplication: {len(df)} records remain.")


#Finally saving.
storePath = CleanDir / f"data_store_{today}.csv"

if storePath.exists():
    existing = pd.read_csv(storePath)
    df = pd.concat([existing, df], ignore_index=True)
    df = df.drop_duplicates(subset=["parameter", "datetimeFrom"])
    logging.info("Merged with existing clean data_store.csv")

df.to_csv(storePath, index=False)
logging.info(f"Clean data saved to {storePath}")


#US: 155
