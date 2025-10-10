from pathlib import Path
import requests
import json
import time
from openaq import OpenAQ
import os
import pandas as pd

#function to save each JSON file.
def SaveJSON(file_path, data):
    with open(file_path, 'w', encoding='utf-8') as f:
        for rec in data:
            f.write(json.dumps(rec.dict(), ensure_ascii=False) + '\n')

#config params
europe_countries = ["GB", "FR", "DE", "IT", "ES", "PL", "NL", "SE", "NO", "FI",
    "BE", "AT", "CH", "DK", "IE", "PT", "CZ", "HU", "GR"]
parameters = ["pm25", "pm10", "no2", "o3"]
date_from = "2025-09-01T00:00:00Z"
date_to   = "2025-09-30T23:59:59Z"
limit = 1000


API_KEY = os.getenv("OPENAQ_API_KEY")
if not API_KEY:
    raise ValueError("API_KEY is not set")
client = OpenAQ(API_KEY)


#set params for call later
MaximumRetries = 5
WaitTime = 1

#check/create raw directory.
Path("data/raw").mkdir(parents=True, exist_ok=True)

#run call attempts.
for country in europe_countries:
    print('Pulling data for', country)
    page = 1

    while True:

        retry = 0
        while retry < MaximumRetries:

            try:
                response = client.measurements.list(country_id=country, parameters=parameters, date_from=date_from,
                                                    date_to=date_to, limit=limit, page=page)
                file_path = f'data/raw/measurements_{country}.jsonl'

                if not response.results:
                    print(f"No results found for {country}.")
                    break

                SaveJSON(file_path, response.results)
                print(f"Successfully saved measurements for {country}.")
                page += 1


            except Exception as e:
                retry += 1
                WaitTime = retry * WaitTime
                if retry > MaximumRetries:
                    print(f"Request Failed: {e}. Retrying in {WaitTime} seconds...")
                    time.sleep(WaitTime)
                else:
                    print(f'Maximum retries reached for {country}.')
                    break
    else:
        print(f'Maximum retries for {country} reached.')


client.close()
#Save raw





CleanedPath = "data/clean/locations_clean.csv"
Path("data/clean").mkdir(parents=True, exist_ok=True)
df.to_csv(CleanedPath, index=False)





#US: 155
