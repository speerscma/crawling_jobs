from google.cloud import storage
import json
import requests
import json
from datetime import datetime as dt
from fake_useragent import UserAgent
import time
from random import randrange

# define headers
def gen_headers():
    headers = {
        "authority":        "www.jobs.ch",
        "method":           "GET",
        "scheme":           "https",
        "accept":           "application/json",
        "accept-encoding":  "gzip, deflate, br",
        "accept-language":  "en",
        "user-agent":       UserAgent().chrome,
    }
    return headers


def get_job_listing(job_id):
    url = f"https://www.jobs.ch/api/v1/public/search/job/{job_id}"
    response = requests.get(url=url, headers=gen_headers())
    sleep_time = randrange(0, 3)
    print(f"Sleeping: {sleep_time}")
    time.sleep(sleep_time)
    return response


# Set the bucket and file name
BUCKET_NAME = 'speers-jobs-ch-raw-json'
GCP_PROJECT = 'speers-spielewiese-dev'


# Connect to the bucket
storage_client = storage.Client(project=GCP_PROJECT)
bucket = storage_client.bucket(BUCKET_NAME)


# for folder name
date = dt.now().strftime('%Y-%m-%d')

with open("job_id_list.json", "r") as f:
    job_id_list = json.loads(f.read())

for i, job_id in enumerate(job_id_list):
    if i > 5816:
        print(f"[{dt.now().strftime('%H:%M:%S')}] {i}/{len(job_id_list)}: {job_id}")
        job_listing = get_job_listing(job_id)
        print(job_id)
        print(job_listing.status_code)
        if job_listing.status_code == 200:
            # Write the JSON string to a file in the bucket
            blob = bucket.blob(f"{date}/{job_id}.json")
            blob.upload_from_string(json.dumps(json.loads(job_listing.content)).encode("utf-8"))

print('Data written to GCS bucket')
