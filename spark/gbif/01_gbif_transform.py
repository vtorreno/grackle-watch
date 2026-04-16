# Great-tailed Grackle | GBIF Bronze → Silver Transform
# 2026 Capstone Project | Grackle Watch | DataTalks.Club
# Author: Victoria Torreno

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import col, count, when
from dotenv import load_dotenv
from pathlib import Path

logging.basicConfig(level=logging.INFO)
load_dotenv()

GCS_BUCKET = os.getenv('GCS_BUCKET')
BRONZE_PATH = f'gs://{GCS_BUCKET}/bronze_gbif/gbif_occurrences'
SILVER_PATH = f'gs://{GCS_BUCKET}/silver_gbif'
JAR_PATH = os.getenv('GCS_CONNECTOR_JAR', 'gcs-connector.jar')

SILVER_COLUMNS = [
    'gbif_id', 'occurrence_id',
    'scientific_name', 'species', 'iucn_red_list_category',
    'decimal_latitude', 'decimal_longitude', 'coordinate_uncertainty_in_meters',
    'state_province', 'county', 'locality',
    'event_date', 'year', 'month', 'day',
    'individual_count', 'occurrence_status', 'basis_of_record', 'sex',
    'dataset_name', 'recorded_by', 'license'
]

def create_spark_session() -> SparkSession:
    return SparkSession.builder \
        .appName('gbif_bronze_to_silver') \
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
    df = df.dropDuplicates(['gbif_id'])
    logging.info(f'Deduplication: removed {initial_ct - df.count():,} duplicates')

    # filter invalid records
    initial_ct = df.count()
    df = df.dropna(subset=['decimal_latitude', 'decimal_longitude', 'event_date'])
    logging.info(f'Filtered {initial_ct - df.count():,} records with missing coordinates or event date')

    # type casting
    df = df \
        .withColumn('year', f.col('year').cast('integer')) \
        .withColumn('month', f.col('month').cast('integer')) \
        .withColumn('day', f.col('day').cast('integer')) \
        .withColumn('individual_count', f.col('individual_count').cast('integer')) \
        .withColumn('event_date', f.col('event_date').cast('date')) \
        .withColumn('gbif_id', f.col('gbif_id').cast('long'))

    # column renaming
    df = df \
        .withColumnRenamed('decimal_latitude', 'latitude') \
        .withColumnRenamed('decimal_longitude', 'longitude') \
        .withColumnRenamed('coordinate_uncertainty_in_meters', 'coordinate_uncertainty') \
        .withColumnRenamed('iucn_red_list_category', 'iucn_category') \
        .withColumnRenamed('occurrence_status', 'status') \
        .withColumnRenamed('basis_of_record', 'record_type') \
        .withColumnRenamed('individual_count', 'count') \
        .withColumnRenamed('recorded_by', 'observer') \
        .withColumnRenamed('dataset_name', 'source_dataset') \
        .withColumnRenamed('state_province', 'state')

    # drop columns too sparse to be useful
    # county: ~99% null, locality: ~93% null
    df = df.drop('county', 'locality')

    # fill null count with 1 (minimum presence assumption)
    df = df.fillna({'count': 1})

    # write to GCS Silver
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