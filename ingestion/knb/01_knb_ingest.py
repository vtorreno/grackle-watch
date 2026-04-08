# Great-tailed Grackle | KNB Ingestion Pipeline 
# 2026 Capstone Project | Grackle Watch | DataTalks.Club
# Author: Victoria Torreno
# Data Source: https://knb.ecoinformatics.org/view/doi:10.5063/F13B5XBC

import os
import click
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv() # load credentials from .env file
GCS_BUCKET = os.getenv("GCS_BUCKET")  # get bucket name from .env
BRONZE_PREFIX = 'bronze_knb'

KNB_FILES = {
    'water_tube': 'data/water_tube.csv',
    'color_test': 'data/color_test.csv',
    'interaction': 'data/interaction.csv'
}

def upload_to_gcs(file_path: str, gcs_path: str, bucket_name: str) -> None:
    """
    Upload a local file to Google Cloud Storage.
    """
    client = storage.Client() # initialize GCS client
    bucket = client.bucket(bucket_name) 
    blob =  bucket.blob(gcs_path)
    blob.upload_from_filename(file_path)  # upload local file to GCS
    
    print(f"Uploaded {file_path} to gs://{bucket_name}/{gcs_path}")

@click.command()
@click.option('--bucket', default=GCS_BUCKET, help='GCS destination bucket')
@click.option('--data-dir', default='data', help='Local directory containing KNB CSV files')
def ingest_knb(bucket: str, data_dir: str) -> None:
    """
    Ingest KNB behavioral experiment CSVs to GCS Bronze layer.

    args:
        bucket (str): GCS destination bucket
        data_dir (str): Local directory containing KNB CSV files
    """
    print(f"Starting KNB ingestion to gs://{bucket}/{BRONZE_PREFIX}/")

    for name, filename in KNB_FILES.items():
        file_path = os.path.join(data_dir, os.path.basename(filename))
        gcs_path = f"{BRONZE_PREFIX}/{name}.csv"
        upload_to_gcs(file_path, gcs_path, bucket)

    print("KNB ingestion complete.")

if __name__ == "__main__":
    ingest_knb()  
