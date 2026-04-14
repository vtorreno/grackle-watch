# Great-tailed Grackle | GBIF Ingestion Pipeline 
# 2026 Capstone Project | Grackle Watch | DataTalks.Club
# Author: Victoria Torreno
# Data Source: https://www.gbif.org/

import os
# show pipeline progress
os.environ.setdefault('RUNTIME__LOG_LEVEL', 'INFO')  

import dlt
import click
from dlt.sources.rest_api import rest_api_source
from dotenv import load_dotenv

# load credentials from .env file
load_dotenv()

SCIENTIFIC_NAME = 'Quiscalus mexicanus'  # Great-tailed Grackle
BASE_URL = 'https://api.gbif.org/v1/'
PAGE_LIMIT = 300 # maximum number of records per page (GBIF API limit)
OCCURRENCE_ENDPOINT = 'occurrence/search'

@click.command()
@click.option('--scientific_name', default=SCIENTIFIC_NAME, help='target species scientific name')
@click.option('--year', default=2025, help='occurrence filter year')
def ingest_gbif(scientific_name: str, year: int) -> None:
    """
    Ingest GBIF occurrence records for a given species and year.

    args:
        scientific_name (str): scientific name of the target species
        year (int): occurrence filter year 
    """
    config = {
        # API configuration
        'client': {
            'base_url': BASE_URL,
        },
        'resources': [
            {
                'name': 'gbif_occurrences',
                'endpoint': {
                    'path': OCCURRENCE_ENDPOINT,
                    'params': {
                        'scientificName': scientific_name,
                        'year': year,
                        'limit': PAGE_LIMIT,  
                    },
                    'paginator': {
                        'type': 'offset',
                        'limit': PAGE_LIMIT,
                        'offset_param': 'offset',
                        'limit_param': 'limit',
                        'total_path': 'count',
                        'maximum_offset': 99_700, # stays within GBIF's 100k offset cap
                    },
                },
                'primary_key': 'key',
                'write_disposition': 'append',
                'max_table_nesting': 0,  # limit nesting to avoid overly complex schemas
            }
        ],
    }

    source = rest_api_source(config)

    pipeline = dlt.pipeline(
        pipeline_name='gbif_pipeline',
        destination='filesystem',
        dataset_name='bronze_gbif'
    )

    print(f'Starting GBIF ingestion: {scientific_name} ({year})')
    # run pipeline to ingest data as parquet
    result = pipeline.run(source, loader_file_format='parquet')
    print(result)

if __name__ == '__main__':
    ingest_gbif()
