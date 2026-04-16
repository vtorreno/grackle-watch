# Great-tailed Grackle | eBird Bronze → Silver Transform
# 2026 Capstone Project | Grackle Watch | DataTalks.Club
# Author: Victoria Torreno

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import count, when, col
from dotenv import load_dotenv
from pathlib import Path

logging.basicConfig(level=logging.INFO)
load_dotenv()

GCS_BUCKET = os.getenv('GCS_BUCKET')
BRONZE_PATH = f'gs://{GCS_BUCKET}/bronze_ebird/ebird_observations'
SILVER_PATH = f'gs://{GCS_BUCKET}/silver_ebird'
JAR_PATH = os.getenv('GCS_CONNECTOR_JAR', 'gcs-connector.jar')

SILVER_COLUMNS = [
    'sub_id', 'loc_id', 'loc_name',
    'lat', 'lng', 'obs_dt',
    'how_many', 'obs_valid', 'obs_reviewed', 'location_private',
    'species_code', 'com_name', 'sci_name',
    'region_code',
]

def create_spark_session() -> SparkSession:
    return SparkSession.builder \
        .appName('ebird_bronze_to_silver') \
        .config('spark.jars', JAR_PATH) \
        .config('spark.hadoop.fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem') \
        .config('spark.hadoop.fs.AbstractFileSystem.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS') \
        .config('spark.hadoop.google.cloud.auth.service.account.enable', 'true') \
        .config('spark.hadoop.google.cloud.auth.service.account.json.keyfile', os.getenv('GOOGLE_APPLICATION_CREDENTIALS')) \
        .config('spark.ui.showConsoleProgress', 'false') \
        .getOrCreate()

def transform(spark: SparkSession) -> None:
    # load Bronze
    logging.info('Reading Bronze data from GCS.')
    df = spark.read.parquet(BRONZE_PATH)
    logging.info(f'Bronze rows: {df.count():,} | Columns: {len(df.columns)}')

    # column pruning
    df = df.select(SILVER_COLUMNS)

    # deduplication
    initial_ct = df.count()
    df = df.dropDuplicates(['sub_id'])
    logging.info(f'Deduplication: removed {initial_ct - df.count():,} duplicates')

    # filter invalid records
    initial_ct = df.count()
    df = df.dropna(subset=['lat', 'lng', 'obs_dt'])
    logging.info(f'Filtered {initial_ct - df.count():,} records with missing coordinates or date')

    # type casting
    df = df \
        .withColumn('obs_dt', f.col('obs_dt').cast('date')) \
        .withColumn('how_many', f.col('how_many').cast('integer'))

    # extract year and month for partitioning
    df = df \
        .withColumn('year', f.year(f.col('obs_dt'))) \
        .withColumn('month', f.month(f.col('obs_dt')))

    # column renaming aligned with GBIF Silver schema for cross-source joins
    df = df \
        .withColumnRenamed('sub_id', 'checklist_id') \
        .withColumnRenamed('loc_id', 'location_id') \
        .withColumnRenamed('loc_name', 'location_name') \
        .withColumnRenamed('lat', 'latitude') \
        .withColumnRenamed('lng', 'longitude') \
        .withColumnRenamed('obs_dt', 'event_date') \
        .withColumnRenamed('how_many', 'count') \
        .withColumnRenamed('obs_valid', 'is_valid') \
        .withColumnRenamed('obs_reviewed', 'is_reviewed') \
        .withColumnRenamed('com_name', 'common_name') \
        .withColumnRenamed('sci_name', 'scientific_name') \
        .withColumnRenamed('region_code', 'state')

    # fill null count with 1 (minimum presence assumption)
    df = df.fillna({'count': 1})

    # write to GCS Silver, partitioned by year and month for efficient querying
    logging.info(f'Writing Silver data to {SILVER_PATH}.')
    df.write \
        .mode('overwrite') \
        .partitionBy('year', 'month') \
        .parquet(SILVER_PATH)

    logging.info(f'Silver data written successfully. Final row count: {df.count():,}')

if __name__ == '__main__':
    spark = create_spark_session()
    spark.sparkContext.setLogLevel('ERROR')
    transform(spark)
    spark.stop()