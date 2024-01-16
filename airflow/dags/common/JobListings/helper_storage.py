import boto3
import os
import socket
import re
import pandas as pd
from io import BytesIO

from io import StringIO
from datetime import datetime, timedelta

# Create a boto3 client with MinIO configuration

AWS_ACCESS_KEY = os.getenv('MINIO_ROOT_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('MINIO_ROOT_SECRET_KEY')
MINIO_IP_ADDRESS = socket.gethostbyname("minio")

HOURS_DELTA = 36

# todo: make the deltatime (1 hour) in is_file_recent a constant


def store_df_to_s3(df, filename, bucket):

    s3_client = boto3.client('s3', endpoint_url=f"http://{MINIO_IP_ADDRESS}:9000",
                             aws_access_key_id=AWS_ACCESS_KEY,
                             aws_secret_access_key=AWS_SECRET_KEY)

    csv_buffer = StringIO()
    df.to_csv(csv_buffer)

    s3_client.put_object(Bucket=bucket,
                         Key=f"{filename}.csv", Body=csv_buffer.getvalue())


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
