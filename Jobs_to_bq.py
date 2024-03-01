from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound
import os
import json

# Set the bucket and file name
BUCKET_NAME = 'speers-jobs-ch-raw-json'
GCP_PROJECT = 'speers-spielewiese-dev'
DATASET_ID = 'jobs_ch'
TABLE_ID = 'jobs_ch_prep'

# Connect to the bucket
storage_client = storage.Client(project=GCP_PROJECT)
bucket = storage_client.bucket(BUCKET_NAME)

# Initialise the BigQuery client
bigquery_client = bigquery.Client(project=GCP_PROJECT)

# Reference to the dataset
dataset_ref = bigquery_client.dataset(DATASET_ID)

# Check if the dataset already exists. If not, create it
try:
    bigquery_client.get_dataset(DATASET_ID)
except NotFound:
    dataset = bigquery.Dataset(dataset_ref)
    dataset = bigquery_client.create_dataset(dataset)
    print(f"Dataset {dataset.dataset_id} created.")

# Reference to the table
table_ref = dataset_ref.table(TABLE_ID)

# Table schema
schema = [
    bigquery.SchemaField('name', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('date', 'DATE', mode='REQUIRED'),
    bigquery.SchemaField('json', 'STRING', mode='REQUIRED'),
]

# Create table in BigQuery
table = bigquery.Table(table_ref, schema=schema)
table = bigquery_client.create_table(table)  # Make an API request.

# Get the blobs(files) in the bucket
blobs = storage_client.list_blobs(BUCKET_NAME)

for blob in blobs:
    # Parse the date and the id from the blob name
    date, job_id_json = os.path.split(blob.name)
    job_id, _ = os.path.splitext(job_id_json)

    # Read the content of the blob
    blob_content = blob.download_as_text()

    # Construct the row to be inserted in BigQuery
    row_to_insert = [
        {"name": job_id, "date": date, "json": blob_content},
    ]

    # Insert row in BigQuery
    errors = bigquery_client.insert_rows_json(table, row_to_insert)

    if errors:
        print(f"Encountered errors while inserting row: {errors}")
    else:
        print(f"Job {job_id} inserted.")

print('Data written to BigQuery')
