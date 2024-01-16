import helper_transform as helper_transform
from constants import JOB_LEVELS

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import udf, col
import os
from io import BytesIO
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from airflow.exceptions import AirflowFailException
import logging
import sys
import re
import boto3
import socket
import pandas as pd

AWS_SPARK_ACCESS_KEY = os.getenv('MINIO_SPARK_ACCESS_KEY')
AWS_SPARK_SECRET_KEY = os.getenv('MINIO_SPARK_SECRET_KEY')
MINIO_IP_ADDRESS = socket.gethostbyname("minio")
SPARK_MASTER_IP_ADDRESS = socket.gethostbyname("spark-master")

BUCKET_FROM = 'bronze'
BUCKET_TO = 'silver'
DELTA_MINUTES = 15


def get_spark_session(appname):

    spark = (SparkSession.builder
             .appName(appname)
             #  .enableHiveSupport()
             .getOrCreate())
    return spark


def get_boto_client():
    return boto3.client(
        's3',
        aws_access_key_id=AWS_SPARK_ACCESS_KEY,
        aws_secret_access_key=AWS_SPARK_SECRET_KEY,
        endpoint_url=f"http://{MINIO_IP_ADDRESS}:9000"
    )


def is_file_recent(filename):
    now = datetime.now()
    hoursdelta = now - timedelta(minutes=DELTA_MINUTES)
    try:
        # filename format should be 'source_raw_datetime_location_keyword'
        filename_splitted = filename.split('_')
        date_str = filename_splitted[2]
        # this is isoformat without special characters "-:"
        file_datetime = datetime.strptime(date_str, '%Y%m%dt%H%M%S%f')
        return file_datetime >= hoursdelta
    except (IndexError, ValueError):
        return False


def get_recent_files_from_s3(bucket_name, source_name):
    s3_client = get_boto_client()

    all_files = s3_client.list_objects_v2(
        Bucket=bucket_name, Prefix=F"{source_name}_raw")
    if len(all_files) == 0:
        raise AirflowFailException('Aborting DAG run due to no files fetched.')

    file_count = len(all_files.get('Contents', []))
    logging.info(f"Total number of files: {file_count}")

    # Filter for recent files
    recent_files = [file['Key'] for file in all_files.get(
        'Contents', []) if is_file_recent(file['Key'])]

    file_count = len(recent_files)
    logging.info(f"Total number of recent files: {file_count}")

    return recent_files


def merge_recent_files_to_df(bucket_name, source_name):
    s3_client = get_boto_client()
    recent_files = get_recent_files_from_s3(bucket_name, source_name)

    df_list = []
    for file_key in recent_files:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        file_content = response['Body'].read()
        df = pd.read_csv(BytesIO(file_content))
        # logging.info(f"Columns in file {file_key}: {df.columns.tolist()}")
        df_list.append(df)

    concatenated_df = pd.concat(df_list, ignore_index=True)
    # logging.info(concatenated_df.columns)
    return concatenated_df

# this is optimized for only one file
# def merge_recent_files_to_df(bucket_name, source_name):
#     s3_client = get_boto_client()
#     recent_files = get_recent_files_from_s3(bucket_name, source_name)
#     response = s3_client.get_object(Bucket=bucket_name, Key=recent_files[0])
#     file_content = response['Body'].read()
#     df = pd.read_csv(BytesIO(file_content))
#     logging.info(f"Columns in file {recent_files[0]}: {df.columns.tolist()}")
#     # df = df.drop("Unnamed: 0", axis=1)
#     return df


def save_to_delta(df, target_path):
    # overwrite mode is important, otherwise errors
    df.write.format("delta").mode("overwrite").save(target_path)


def load_recent_files(bucket_name, source_name):
    return merge_recent_files_to_df(bucket_name, source_name)


def transform_job_level(level, title):
    return helper_transform.transform_job_level(level, title) if level else JOB_LEVELS["middle"]


def clean_linkedin_id(linkedin_id):
    return linkedin_id.replace('<!--', '').replace('-->', '') if linkedin_id else None


def clean_company_linkedin_url(company_url):
    return company_url.split('?')[0] if company_url else None


def transform(df):

    df_filtered = df.dropna()
    # df_filtered.limit(20).show(truncate=False)

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
    # todo: this has to be modified, because there are certain wording which indicates vague applicants
    extract_applicants_udf = udf(lambda x: int(re.compile(
        r'\d+').findall(x)[0]) if x and re.compile(r'\d+').findall(x) else 0, IntegerType())
    transform_job_level_udf = udf(transform_job_level, StringType())
    clean_linkedin_id_udf = udf(clean_linkedin_id, StringType())
    clean_company_linkedin_url_udf = udf(
        clean_company_linkedin_url, StringType())

    df_cleaned = df_filtered.withColumn("title", transform_job_title_udf(col("title"))) \
        .withColumn("level", transform_job_level_udf(col("level"), col("title"))) \
        .withColumn("location", transform_job_location_udf(col("location"))) \
        .withColumn("publish_date", transform_to_isoformat_udf(col("publish_date"), col("search_datetime"))) \
        .withColumn("applicants", extract_applicants_udf(col("applicants"))) \
        .withColumn("language", transform_detect_language_udf(col("description"))) \
        .withColumn("linkedin_id", clean_linkedin_id_udf(col("linkedin_id"))) \
        .withColumn("company_linkedin_url", clean_company_linkedin_url_udf(col("company_linkedin_url"))) \
        .withColumnRenamed("Unnamed: 0", "Index")  # important, otherwise error. spark needs all columns to be named

    return df_cleaned


def get_df_schema():
    schema = StructType([
        StructField("Unnamed: 0", IntegerType(), True),
        StructField("company_name", StringType(), True),
        StructField("company_linkedin_url", StringType(), True),
        StructField("title", StringType(), True),
        StructField("location", StringType(), True),
        StructField("linkedin_id", StringType(), True),
        StructField("url", StringType(), True),
        StructField("applicants", StringType(), True),
        StructField("publish_date", StringType(), True),
        StructField("level", StringType(), True),
        StructField("employment", StringType(), True),
        StructField("function", StringType(), True),
        StructField("industries", StringType(), True),
        StructField("description", StringType(), True),
        # StructField("search_datetime", TimestampType(), True),
        StructField("search_datetime", StringType(), True),
        StructField("search_keyword", StringType(), True),
        StructField("search_location", StringType(), True),
    ])
    return schema


def main(args):
    # get the parameters from SparkSubmitOperator
    spark_session_name = args[1]
    source_name = args[2]

    spark = get_spark_session(spark_session_name)
    data_raw = load_recent_files(BUCKET_FROM, source_name)

    # convert pandas df to spark df
    schema = get_df_schema()
    spark_df = spark.createDataFrame(data_raw, schema=schema)

    data_clean = transform(spark_df)

    target_path = f"s3a://{BUCKET_TO}/{source_name}_data"
    save_to_delta(data_clean, target_path)

    spark.stop()


if __name__ == "__main__":
    main(sys.argv)
