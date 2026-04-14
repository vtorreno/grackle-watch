# Great-tailed Grackle | eBird Ingestion Pipeline
# 2026 Capstone Project | Grackle Watch | DataTalks.Club
# Author: Victoria Torreno
# Data Source: https://ebird.org/data/download

import os
os.environ.setdefault('RUNTIME__LOG_LEVEL', 'INFO')  # set log level to INFO for better visibility of progress

import dlt
import click
from dlt.sources.rest_api import rest_api_source
from dotenv import load_dotenv
from ebird.api.requests import get_observations

# load credentials from .env file
load_dotenv()

SPECIES_CODE = 'grtgra' # great-tailed grackle
TARGET_REGIONS = ['US-AZ', 'US-TX', 'US-CA', 'US-NM', 'US-OK']
BASE_URL = "https://api.ebird.org/v2/"
BACK_DAYS = 30 # rolling observation window

@dlt.resource(name='ebird_observations', write_disposition='append', primary_key='subId')   
def ebird_observations(api_key: str, regions: list, back: int):
    """ Fetch eBird observations per region. """
    for region in regions:
        endpoint = f'data/obs/{region}/recent/{SPECIES_CODE}'
        response = rest_api_source({
            'client': {
                'base_url': BASE_URL,
                'headers': {'x-ebirdapitoken': api_key},
            },
            'resources': [
                {
                    'name': 'obs',
                    'endpoint': {
                        'path': endpoint,
                        'params': {'back': back},
                    },
                }
            ],
        })
        for record in response.resources['obs']:
             record['region_code'] = region  # add region info to each record
             yield record

@click.command()
@click.option('--back', default=BACK_DAYS, help='number of days back to fetch observations')
def ingest_ebird(back: int) -> None:
    """
    Ingest eBird occurrence records for Great-tailed Grackle across target regions.

    args:
        back (int): number of days back to fetch observations (max 30 days per eBird API limits)
    """
    api_key = os.getenv('EBIRD_API_KEY')

    pipeline = dlt.pipeline(
        pipeline_name='ebird_pipeline',
        destination='filesystem',
        dataset_name='bronze_ebird'
    )
    print(f'Starting eBird ingestion: {SPECIES_CODE} ({back} days, {len(TARGET_REGIONS)} regions)')
    # run pipeline to ingest data as parquet
    result = pipeline.run(ebird_observations(api_key, TARGET_REGIONS, back), loader_file_format='parquet')
    print(result)

if __name__ == '__main__':
    ingest_ebird()
