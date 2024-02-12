from io import BytesIO
from datetime import datetime, timedelta
import logging
import pandas as pd


class FileProcessing:
    def __init__(self, s3_client, bucket_name, source_name, delta_minutes):
        self.s3_client = s3_client
        self.bucket_name = bucket_name
        self.source_name = source_name
        self.delta_minutes = delta_minutes

    def load_recent_files(self):
        return self.merge_recent_files_to_df(self.bucket_name, self.source_name)

    def is_file_recent(self, filename):
        now = datetime.now()
        hoursdelta = now - timedelta(minutes=self.delta_minutes)
        try:
            # filename format should be 'source_raw_datetime_location_keyword'
            filename_splitted = filename.split('_')
            date_str = filename_splitted[2]
            # this is isoformat without special characters "-:"
            file_datetime = datetime.strptime(date_str, '%Y%m%dt%H%M%S%f')
            return file_datetime >= hoursdelta
        except (IndexError, ValueError):
            return False

    def get_recent_files_from_s3(self, bucket_name, source_name):
        # s3_client = self.get_boto_client()

        all_files = self.s3_client.list_objects_v2(
            Bucket=bucket_name, Prefix=F"{source_name}_raw")
        # if len(all_files) == 0:
        #     raise AirflowFailException(
        #         'Aborting DAG run due to no files fetched.')

        file_count = len(all_files.get('Contents', []))
        logging.info(f"Total number of files: {file_count}")

        # Filter for recent files
        recent_files = [file['Key'] for file in all_files.get(
            'Contents', []) if self.is_file_recent(file['Key'])]

        file_count = len(recent_files)
        logging.info(f"Total number of recent files: {file_count}")

        return recent_files

    def merge_recent_files_to_df(self, bucket_name, source_name):
        # s3_client = self.get_boto_client()
        recent_files = self.get_recent_files_from_s3(bucket_name, source_name)
        df_list = []

        for file_key in recent_files:
            response = self.s3_client.get_object(
                Bucket=bucket_name, Key=file_key)
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
