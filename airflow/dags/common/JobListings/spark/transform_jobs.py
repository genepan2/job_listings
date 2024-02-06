from spark_session_manager import SparkSessionManager
from s3_client_manager import S3ClientManager
from file_processing import FileProcessing
from data_storage import DataStorage
from data_transformation import DataTransformation
from data_enrichment import DataEnrichment

import os

# from airflow.exceptions import AirflowFailException
import logging
import sys
import socket


if __name__ == "__main__":
    args = sys.argv
    appname = args[1]
    source_name = args[2]
    bucket_from = args[3]
    bucket_to = args[4]
    delta_minutes = int(args[5])

    spark_manager = SparkSessionManager(appname)
    spark_session = spark_manager.get_spark_session()

    AWS_SPARK_ACCESS_KEY = os.getenv('MINIO_SPARK_ACCESS_KEY')
    AWS_SPARK_SECRET_KEY = os.getenv('MINIO_SPARK_SECRET_KEY')
    MINIO_IP_ADDRESS = socket.gethostbyname("minio")
    SPARK_MASTER_IP_ADDRESS = socket.gethostbyname("spark-master")

    s3_client_manager = S3ClientManager(
        AWS_SPARK_ACCESS_KEY, AWS_SPARK_SECRET_KEY, f"http://{MINIO_IP_ADDRESS}:9000")
    s3_client = s3_client_manager.get_boto_client()

    file_processor = FileProcessing(
        s3_client, bucket_from, source_name, delta_minutes)

    data_storage = DataStorage()

    data_transformation = DataTransformation(args)

    # Der Hauptablauf würde hier folgen, z.B.:
    data_raw = file_processor.load_recent_files()

    schema = data_transformation.get_df_schema(source_name)
    spark_df = spark_session.createDataFrame(data_raw, schema=schema)

    data_clean = data_transformation.transform(spark_df)

    target_path_delta = f"s3a://{bucket_to}/{source_name}_data"
    target_path_csv = f"s3a://{bucket_to}/{source_name}_data_csv"

    data_storage.save_to_delta(data_clean, target_path_delta)
    data_storage.save_as_csv(data_clean, target_path_csv)

    spark_manager.stop()
