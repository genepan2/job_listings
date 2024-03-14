from airflow import AirflowException, AirflowFailException
import pandas as pd
from io import StringIO

from job_config_constants import FIELDS
from job_helper_storage import store_df_to_s3, get_boto_client
from job_helper_utils import create_key_name


class JobLoader:
    def __init__(self, source_name):
        self.source_name = source_name

    def read_csv_from_s3(self, bucket, folder):
        s3_client = get_boto_client()
        objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=f"{folder}/")

        csv_objects = [obj for obj in objects['Contents']
                       if obj['Key'].endswith('.csv')]
        sorted_csv_objects = sorted(
            csv_objects, key=lambda x: x['LastModified'], reverse=True)

        # Identify all files with the common part after the 'part-00000-'
        part_zero_file = next(
            (obj for obj in sorted_csv_objects if "part-00000-" in obj['Key']), None)
        if part_zero_file:
            unique_part = part_zero_file['Key'].split("part-00000-")[1]
            latest_files = [
                obj for obj in sorted_csv_objects if unique_part in obj['Key']]

            if not latest_files:
                raise AirflowException(
                    "No CSV Files found which fit the creteria.")

            return latest_files

        # return []
        # raise AirflowFailException(f"No 'part-00000-' CSV-File found in {bucket} Bucket.")
        raise AirflowException(
            f"No 'part-00000-' CSV-File found in {bucket} Bucket.")

    def concat_csv_files(self, files, bucket, folder):
        s3_client = get_boto_client()
        all_dataframes = []

        for file in files:
            obj = s3_client.get_object(
                Bucket=bucket, Prefix=f"{folder}/", Key=file['Key'])
            content = obj['Body'].read().decode('utf-8')

            df = pd.read_csv(StringIO(content))
            all_dataframes.append(df)

        concatenated_df = pd.concat(all_dataframes, ignore_index=True)
        return concatenated_df

    def prepare_data(self, data):
        return True

    def load_data_to_dw(self, data):
        return True

    def main(self):
        bucket = 'silver'
        folder = f"{self.source_name}_data_csv"

        last_csv_files_batch = self.read_csv_from_s3(
            bucket, folder)

        data_df = self.concat_csv_files(last_csv_files_batch, bucket, folder)
