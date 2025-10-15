
#Declaring modules.
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

#google and reddit have saved my butt several times through this.

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
def SaveJSON(file_path, data, sensor_id=None, sensor_name=None, location_id=None, state=None, append=True):
    logging.info('Saving RAW JSON')
    mode = 'a' if append else 'w'
    with open(file_path, mode, encoding='utf-8') as f:
        for rec in data:
            # attach metadata if available
            if isinstance(rec, dict):
                rec['sensor_id'] = sensor_id
                rec['sensor_name'] = sensor_name
                rec['location_id'] = location_id
                rec['state'] = state
            f.write(json.dumps(rec, ensure_ascii=False) + '\n')
    logging.info('Saved RAW JSON')


def retrieval(d, *keys):
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

# logging.info("Starting data fetch for ME)")
# for state, bbox in state_bboxes.items():
#     logging.info(f"Fetching {state} (bbox: {bbox})")
#     page = 1
#     retry = 0
#
#     while True:
#         params = {"bbox": bbox, "limit": limit, "page": page}
#         try:
#             retry+=1
#             response = requests.get(locationsURL, headers=headers, params=params, timeout=30)
#
#             if response.status_code == 429:
#                 logging.warning(f"{state} page {page}: rate limit hit, sleeping 10s")
#                 time.sleep(10)
#                 continue
#             elif response.status_code >= 500:
#                 logging.error(f"{state} page {page}: server error {response.status_code}, skipping.")
#                 break
#
#             response.raise_for_status()
#             results = response.json().get("results", [])
#             if not results:
#                 logging.info(f"{state}: no more results after page {page-1}")
#                 break
#
#             #Iterate over each location to pull sensor information
#             for loc in results:
#                 for sensor in loc.get("sensors", []):
#                     sensor_info = {
#                         "state": state,
#                         "location_id": loc.get("id"),
#                         "sensor_id": sensor.get("id"),
#                         "parameter": sensor.get("parameter"),
#                         "sensor_name": loc.get("name"),
#                         "coordinates": loc.get("coordinates"),
#                     }
#                     allSensors.append(sensor_info)
#
#             logging.info(f"{state} page {page}: fetched {len(results)} locations.")
#
#             #Store avail sensors
#             locations.extend(results)
#             page += 1
#             time.sleep(WaitTime)
#
#         except requests.exceptions.RequestException as e:
#             if retry >= MaximumRetries:
#                 logging.error(f"{state} page {page}: Request Failed {e}, Maximum attempts reached: Skipping.")
#                 break
#             else:
#                 retry+=1
#                 logging.warning(f"{state} page {page}: request failed ({e}), retrying...")
#                 time.sleep(WaitTime * retry)
#                 continue
#
# logging.info(f"Finished fetching. Total locations: {len(locations)}")
#
# for sensor in allSensors:
#     sensor_id = sensor["sensor_id"]
#     sensor_name = sensor["sensor_name"]
#     state = sensor["state"]
#
#     logging.info(f"Pulling data for {sensor_name} ({state}) sensor {sensor_id}")
#     page = 1
#     total_measurements = 0
#     retry = 0
#     WaitTime = 2
#     today = datetime.now(ZoneInfo("UTC")).strftime("%Y-%m-%d")
#
#     while True:
#         params = {
#             "date_from": date_from,
#             "date_to": date_to,
#             "limit": limit,
#             "page": page
#         }
#         URL = f"https://api.openaq.org/v3/sensors/{sensor_id}/days"
#         try:
#             resp = requests.get(URL, headers=headers, params=params, timeout=30)
#             if resp.status_code == 404:
#                 logging.warning(f"Sensor {sensor_id} not found.")
#                 break
#
#             resp.raise_for_status()
#             results = resp.json().get("results", [])
#             if not results:
#                 logging.info(f"{sensor_id}: no more results after page {page - 1}")
#                 break
#
#             total_measurements += len(results)
#             RawFilePath = RawDir / f"{today}.jsonl"
#             SaveJSON(RawFilePath, results, sensor_id=sensor_id, sensor_name=sensor_name, location_id=sensor.get("location_id"), state=state)
#
#             logging.info(f"Page {page}: saved {len(results)} measurements to {RawFilePath}")
#
#             page += 1
#             time.sleep(1)
#
#         except requests.exceptions.RequestException as e:
#             if retry >= MaximumRetries:
#                 logging.error(f'Error fetching sensor {sensor_id} page {page}: Request Failed {e}, Maximum attempts reached. Moving on.')
#                 break
#             else:
#                 retry+=1
#                 logging.error(f"Error fetching sensor {sensor_id} page {page}: {e} retrying...")
#                 time.sleep(WaitTime * retry)
#             continue
#
#     logging.info(f"Done — {total_measurements} total measurements")
# logging.info("Data fetch complete!")


#Cleaning time.

today = datetime.now(ZoneInfo("UTC")).strftime("%Y-%m-%d")

#Defining file just created
NewFile = RawDir / f'{today}.jsonl'
if not NewFile.exists():
    logging.error(f"Raw file {NewFile} not found. Exiting.")
    raise SystemExit(1)

records = []
with open(NewFile, "r", encoding="utf-8") as f:
    for line in f:
        try:
            rec = json.loads(line)

            # Direct extraction for actual JSON structure
            sensor_id = rec.get("sensor_id")
            sensor_name = rec.get("sensor_name")
            location_id = rec.get("location_id")
            state = rec.get("state")
            sensor_tag = rec.get("parameter", {}).get("name")      # ex: "o3"
            avg_value = rec.get("summary", {}).get("avg")          # ex: 0.0175
            datetime_from = rec.get("period", {}).get("datetimeFrom", {}).get("utc")
            datetime_to = rec.get("period", {}).get("datetimeTo", {}).get("utc")

            if sensor_id and sensor_tag and avg_value is not None:
                records.append({
                    "sensor_id": sensor_id,
                    "sensor_name": sensor_name,
                    "location_id": location_id,
                    "state": state,
                    "sensor_tag": sensor_tag,
                    "avg_value": avg_value,
                    "datetime_from": datetime_from,
                    "datetime_to": datetime_to
                })
        except json.JSONDecodeError:
            logging.warning(f"Malformed JSON line in {NewFile.name}")

if not records:
    logging.error("No valid records found in today's file. Exiting.")
    raise SystemExit(1)

df = pd.DataFrame(records)
df["avg_value"] = pd.to_numeric(df["avg_value"], errors="coerce")
df["datetime_from"] = pd.to_datetime(df["datetime_from"], utc=True, errors="coerce")
df["datetime_to"] = pd.to_datetime(df["datetime_to"], utc=True, errors="coerce")

# drop blanks and duplicates
before = len(df)
df = df.dropna(subset=["avg_value", "datetime_from", "sensor_tag"])
df = df.drop_duplicates(subset=["sensor_id", "datetime_from"])
after = len(df)

logging.info(f"Cleaned {before - after} duplicates or invalid rows; {after} remain.")

# save cleaned file
CleanDir.mkdir(parents=True, exist_ok=True)
clean_path = CleanDir / f"cleaned_data_{today}.csv"
df.to_csv(clean_path, index=False)
logging.info(f"Today's cleaned data saved to {clean_path}")

#Finally saving.
storePath = CleanDir / "data_store.csv"
if storePath.exists() and storePath.stat().st_size > 0:
    existing = pd.read_csv(storePath)
    df = pd.concat([existing, df], ignore_index=True)
    df = df.drop_duplicates(subset=["sensor_id", "datetime_from"])
    logging.info("Merged with existing clean data_store.csv")

df.to_csv(storePath, index=False)
logging.info(f"Clean data saved to {storePath}")






#US: 155
