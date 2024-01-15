from pyspark.sql import SparkSession
import sys
import re
import boto3
import socket
import pandas as pd
from datetime import datetime, timedelta
from io import BytesIO
import os
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, IntegerType
import pyspark.sql.functions as F

# from constants import PATH, FIELDS, JOB_LOCATIONS, JOB_LEVELS
from constants import JOB_LEVELS
import helper_transform as helper_transform

AWS_SPARK_ACCESS_KEY = os.getenv('MINIO_SPARK_ACCESS_KEY')
AWS_SPARK_SECRET_KEY = os.getenv('MINIO_SPARK_SECRET_KEY')
MINIO_IP_ADDRESS = socket.gethostbyname("minio")

HOURS_DELTA = 48


def get_spark_session(appname, endpoint_url,
                      access_key, secret_key):

    spark = (SparkSession.builder
             .appName(appname)
             .config("spark.network.timeout", "10000s")
             #  .config("hive.metastore.uris", hive_metastore)
             #  .config("hive.exec.dynamic.partition", "true")
             #  .config("hive.exec.dynamic.partition.mode", "nonstrict")
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
             .config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "true")
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
             .config("spark.hadoop.fs.s3a.fast.upload", "true")
             .config("spark.hadoop.fs.s3a.endpoint", endpoint_url)
             .config("spark.hadoop.fs.s3a.access.key", access_key)
             .config("spark.hadoop.fs.s3a.secret.key", secret_key)
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.history.fs.logDirectory", "s3a://spark/")
             .config("spark.sql.files.ignoreMissingFiles", "true")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
             #  .enableHiveSupport()
             .getOrCreate())
    return spark


def is_file_recent(filename, source_name):
    now = datetime.now()
    one_hour_ago = now - timedelta(hours=HOURS_DELTA)
    try:
        # filename format should be 'source_datetime_location_keyword'
        filename_splitted = filename.split('_')
        source = filename_splitted[0]
        date_str = filename_splitted[1]
        # this is isoformat without special characters (-:)
        file_datetime = datetime.strptime(date_str, '%Y%m%dt%H%M%S')
        return file_datetime.date() == now.date() and file_datetime >= one_hour_ago and source == source_name
    except (IndexError, ValueError):
        return False


def get_recent_files_from_s3(bucket_name, source_name):
    s3_client = boto3.client('s3')

    # List files in the bucket
    all_files = s3_client.list_objects_v2(Bucket=bucket_name)

    # Filter for recent files
    recent_files = [file['Key'] for file in all_files.get(
        'Contents', []) if is_file_recent(file['Key'], source_name)]

    return recent_files


def merge_recent_files_to_df(bucket_name, source_name):
    s3_client = boto3.client('s3')
    recent_files = get_recent_files_from_s3(bucket_name, source_name)

    df_list = []
    for file_key in recent_files:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        file_content = response['Body'].read()
        df = pd.read_csv(BytesIO(file_content))
        df_list.append(df)

    return pd.concat(df_list, ignore_index=True)


def save_to_delta(df, target_path):
    # Save the transformed DataFrame to Delta Lake format
    df.write.format("delta").save(target_path)


def load_recent_files(self, bucket_name, source_name):
    # Function to load recent files from S3 and return a DataFrame
    return merge_recent_files_to_df(bucket_name, source_name)


def transform_job_level(level, title):
    # Include your existing logic here and return MIDDLE_LEVEL if level is None
    return helper_transform.transform_job_level(level, title) if level else JOB_LEVELS["middle"]


def clean_linkedin_id(linkedin_id):
    return linkedin_id.replace('<!--', '').replace('-->', '') if linkedin_id else None


def clean_company_linkedin_url(company_url):
    return company_url.split('?')[0] if company_url else None


def transform(self, df):

    # clean_string_udf = udf(HelperTransform.clean_string, StringType())
    transform_job_title_udf = udf(
        helper_transform.transform_job_title, StringType())
    # transform_job_level_udf = udf(HelperTransform.transform_job_level, StringType())
    transform_job_location_udf = udf(
        helper_transform.transform_job_location, StringType())
    transform_to_isoformat_udf = udf(
        helper_transform.transform_to_isoformat, StringType())
    transform_detect_language_udf = udf(
        helper_transform.transform_detect_language, StringType())
    extract_applicants_udf = udf(lambda x: int(re.compile(
        r'\d+').findall(x)[0]) if x else 0, IntegerType())
    transform_job_level_udf = udf(self.transform_job_level, StringType())
    clean_linkedin_id_udf = udf(self.clean_linkedin_id, StringType())
    clean_company_linkedin_url_udf = udf(
        self.clean_company_linkedin_url, StringType())

    # Apply transformations
    df_cleaned = df.withColumn("title", transform_job_title_udf(col("title"))) \
        .withColumn("level", transform_job_level_udf(col("level"), col("title"))) \
        .withColumn("location", transform_job_location_udf(col("location"))) \
        .withColumn("publish_date", transform_to_isoformat_udf(col("publish_date"), col("search_datetime"))) \
        .withColumn("applicants", extract_applicants_udf(col("applicants"))) \
        .withColumn("language", transform_detect_language_udf(col("description"))) \
        .withColumn("linkedin_id", clean_linkedin_id_udf(col("linkedin_id"))) \
        .withColumn("company_linkedin_url", clean_company_linkedin_url_udf(col("company_linkedin_url")))

    return df_cleaned


def main(args):
    source_name = args[1]
    bucket_name = args[2]
    source_name = args[3]
    # Initialize a Spark session
    spark = SparkSession.builder.appName("ExampleApp").getOrCreate()

    spark = get_spark_session(
        'LinkedInTransformer', f"http://{MINIO_IP_ADDRESS}:9000", AWS_SPARK_ACCESS_KEY, AWS_SPARK_SECRET_KEY)
    data_raw = load_recent_files('bronze', 'linkedin')
    data_clean = transform(data_raw)
    # target_path = create_key_name('linkedin', True)
    # target_path = "s3a://my-datalake-bucket/silver/linkedin_data"
    save_to_delta(data_clean, "s3a://silver/linkedin_data")

    spark.stop()


if __name__ == "__main__":
    main(sys.argv)
