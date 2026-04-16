# Great-tailed Grackle | KNB Bronze → Silver Transform
# 2026 Capstone Project | Grackle Watch | DataTalks.Club
# Author: Victoria Torreno

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from dotenv import load_dotenv
from pathlib import Path

logging.basicConfig(level=logging.INFO)
load_dotenv()

GCS_BUCKET = os.getenv('GCS_BUCKET')
BRONZE_PATH = f'gs://{GCS_BUCKET}/bronze_knb'
SILVER_PATH = f'gs://{GCS_BUCKET}/silver_knb'
JAR_PATH = os.getenv('GCS_CONNECTOR_JAR', 'gcs-connector.jar')

WATER_TUBE_COLUMNS = [
    'experiment', 'date', 'batch',
    'bird', 'trial', 'choice_number',
    'choice_correct', 'choice',
    'extracted_food', 'water_level',
    'tube_on_left', 'notes'
]


def create_spark_session() -> SparkSession:
    return SparkSession.builder \
        .appName('knb_bronze_to_silver') \
        .config('spark.jars', JAR_PATH) \
        .config('spark.hadoop.fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem') \
        .config('spark.hadoop.fs.AbstractFileSystem.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS') \
        .config('spark.hadoop.google.cloud.auth.service.account.enable', 'true') \
        .config('spark.hadoop.google.cloud.auth.service.account.json.keyfile', os.getenv('GOOGLE_APPLICATION_CREDENTIALS')) \
        .config('spark.ui.showConsoleProgress', 'false') \
        .getOrCreate()


def transform_water_tube(spark: SparkSession) -> None:
    logging.info('Transforming `water_tube`.')
    df = spark.read \
        .option('header', 'false') \
        .option('inferSchema', 'true') \
        .csv(f'{BRONZE_PATH}/water_tube.csv')

    # assign column names from KNB documentation
    df = df.toDF(*WATER_TUBE_COLUMNS)

    # drop sparse column
    df = df.drop('notes')

    # type casting
    df = df \
        .withColumn('date', f.to_date(f.col('date'), 'd-MMM-yy')) \
        .withColumn('batch', f.col('batch').cast('integer')) \
        .withColumn('trial', f.col('trial').cast('integer')) \
        .withColumn('choice_number', f.col('choice_number').cast('integer')) \
        .withColumn('water_level', f.col('water_level').cast('double')) \
        .withColumn('choice_correct', (f.col('choice_correct') == 'Yes').cast('boolean')) \
        .withColumn('extracted_food', (f.col('extracted_food') == 'Yes').cast('boolean'))

    # deduplication
    initial_ct = df.count()
    df = df.dropDuplicates()
    logging.info(f'water_tube: removed {initial_ct - df.count():,} duplicates, {df.count():,} remaining')

    df.write.mode('overwrite').parquet(f'{SILVER_PATH}/water_tube')
    logging.info(f'water_tube written: {df.count():,} rows')


def transform_color_test(spark: SparkSession) -> None:
    logging.info('Transforming `color_test`.')
    df = spark.read \
        .option('header', 'true') \
        .option('inferSchema', 'true') \
        .csv(f'{BRONZE_PATH}/color_test.csv')

    # standardize to snake_case
    df = df.toDF(*[c.lower().replace(' ', '_').replace('?', '').strip() for c in df.columns])

    # drop sparse and redundant columns
    df = df.drop('nonoverlappingwindow4-trialbins', 'criterion', 'preference_notes', 'correctchoice')

    # rename for clarity
    df = df \
        .withColumnRenamed('coloronleft', 'color_on_left') \
        .withColumnRenamed('correct', 'is_correct')

    # type casting
    df = df \
        .withColumn('date', f.to_date(f.col('date'), 'd-MMM-yy')) \
        .withColumn('batch', f.col('batch').cast('integer')) \
        .withColumn('trial', f.col('trial').cast('integer')) \
        .withColumn('is_correct', f.col('is_correct').cast('boolean'))

    # deduplication
    initial_ct = df.count()
    df = df.dropDuplicates()
    logging.info(f'color_test: removed {initial_ct - df.count():,} duplicates, {df.count():,} remaining')

    df.write.mode('overwrite').parquet(f'{SILVER_PATH}/color_test')
    logging.info(f'color_test written: {df.count():,} rows')


def transform_interaction(spark: SparkSession) -> None:
    logging.info('Transforming `interaction`.')
    df = spark.read \
        .option('header', 'true') \
        .option('inferSchema', 'true') \
        .csv(f'{BRONZE_PATH}/interaction.csv')

    # standardize to snake_case
    df = df.toDF(*[c.lower().replace(' ', '_').replace('?', '').strip() for c in df.columns])

    # rename for clarity
    df = df \
        .withColumnRenamed('optiononleft', 'option_on_left') \
        .withColumnRenamed('approachedfirst', 'approached_first') \
        .withColumnRenamed('putmorefoodon', 'put_more_food_on')

    # type casting
    df = df \
        .withColumn('date', f.to_date(f.col('date'), 'd-MMM-yy')) \
        .withColumn('trial', f.col('trial').cast('integer'))

    # deduplication
    initial_ct = df.count()
    df = df.dropDuplicates()
    logging.info(f'interaction: removed {initial_ct - df.count():,} duplicates, {df.count():,} remaining')

    df.write.mode('overwrite').parquet(f'{SILVER_PATH}/interaction')
    logging.info(f'interaction written: {df.count():,} rows')


if __name__ == '__main__':
    spark = create_spark_session()
    spark.sparkContext.setLogLevel('ERROR')
    transform_water_tube(spark)
    transform_color_test(spark)
    transform_interaction(spark)
    spark.stop()
    logging.info('KNB Bronze to Silver transform complete.')