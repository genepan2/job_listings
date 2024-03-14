from io import BytesIO
from datetime import datetime, timedelta
import logging
import pandas as pd


class JobFileProcessing:
    def __init__(self, s3_client_manager, bucket_name, source_name, delta_minutes=None):
        self.s3_client_manager = s3_client_manager
        self.bucket_name = bucket_name
        self.source_name = source_name
        self.delta_minutes = delta_minutes

    # def load_recent_files(self):
    #     return self.merge_recent_files_to_df(self.bucket_name, self.source_name)

    def is_file_recent(self, filename):
        # Wenn delta_minutes None ist, betrachte alle Dateien als aktuell
        if self.delta_minutes is None:
            return True

        now = datetime.now()
        cutoff_time = now - timedelta(minutes=self.delta_minutes)
        try:
            # Versuche, das Datum aus dem Dateinamen zu extrahieren und zu prüfen, ob es innerhalb des Zeitfensters liegt
            # filename format should be 'source_raw_datetime_location_keyword'
            date_str = filename.split("_")[2]
            file_datetime = datetime.strptime(date_str, "%Y%m%dT%H%M%S%f")
            return file_datetime >= cutoff_time
        except (IndexError, ValueError) as e:
            logging.error(f"Error parsing date from filename {filename}: {str(e)}")
            return False

    def get_files(self, table_name, file_format):
        prefix = f"{self.source_name}/csv/"
        if self.bucket_name == "bronze":
            prefix += f"{self.source_name}_raw"
        else:  # Für Silver und andere Bucket-Typen kann die Struktur hier angepasst werden
            # Beispiel, tatsächlicher Wert könnte variieren
            prefix += f"{table_name}/part"

        files = self.s3_client_manager.list_objects(self.bucket_name, prefix)
        # Filtert die Dateien basierend auf dem Zeitfenster, wenn delta_minutes gesetzt ist, sonst werden alle Dateien zurückgegeben
        return [
            file["Key"]
            for file in files
            if (
                file["Key"].endswith(f".{file_format}")
                and self.is_file_recent(file["Key"])
            )
        ]

    # def get_recent_files_from_s3(self, bucket_name, source_name):
    #     # this is the new structure in bronze bucket
    #     prefix = f"{source_name}/csv/{source_name}_raw"

    #     all_files = self.s3_client.list_objects_v2(
    #         Bucket=bucket_name, Prefix=prefix)
    #     # if len(all_files) == 0:
    #     #     raise AirflowFailException(
    #     #         'Aborting DAG run due to no files fetched.')

    #     file_count = len(all_files.get('Contents', []))
    #     logging.info(f"Total number of files: {file_count}")

    #     # Filter for recent files
    #     recent_files = [file['Key'] for file in all_files.get(
    #         'Contents', []) if self.is_file_recent(file['Key'])]

    #     file_count = len(recent_files)
    #     logging.info(f"Total number of recent files: {file_count}")

    #     return recent_files

    # def merge_recent_files_to_df(self, bucket_name, source_name):
    #     # s3_client = self.get_boto_client()
    #     recent_files = self.get_recent_files_from_s3(bucket_name, source_name)
    #     df_list = []

    #     for file_key in recent_files:
    #         response = self.s3_client.get_object(
    #             Bucket=bucket_name, Key=file_key)
    #         file_content = response['Body'].read()
    #         df = pd.read_csv(BytesIO(file_content))
    #         # logging.info(f"Columns in file {file_key}: {df.columns.tolist()}")
    #         df_list.append(df)

    #     concatenated_df = pd.concat(df_list, ignore_index=True)
    #     # logging.info(concatenated_df.columns)
    #     return concatenated_df

    def merge_files_to_df(self, table_name=None, file_format="csv"):
        # files = self.get_files(bucket_type)
        files = self.get_files(table_name, file_format)
        if not files:
            raise ValueError("No files were found...")

        df_list = []
        for file_key in files:
            logging.info(f"Processing {file_key}...")
            file_content = self.s3_client_manager.get_object(self.bucket_name, file_key)
            df = pd.read_csv(
                BytesIO(file_content), on_bad_lines="warn", escapechar="\\"
            )
            df_list.append(df)

        if df_list:
            concatenated_df = pd.concat(df_list, ignore_index=True)
            return concatenated_df
        else:
            logging.info("No files to process.")
            # Gibt ein leeres DataFrame zurück, wenn keine Dateien zu verarbeiten sind
            return pd.DataFrame()

    # def load_all_files_from_folder(self, prefix):

    #     if not prefix:
    #         raise ValueError("Prefix was not set")

    #     all_files = self.s3_client.list_objects_v2(
    #         Bucket=self.bucket_name, Prefix=prefix)

    #     df_list = []

    #     for file_key in all_files:
    #         response = self.s3_client.get_object(
    #             Bucket=self.bucket_name, Key=file_key)
    #         file_content = response['Body'].read()
    #         df = pd.read_csv(BytesIO(file_content))
    #         # logging.info(f"Columns in file {file_key}: {df.columns.tolist()}")
    #         df_list.append(df)

    #     concatenated_df = pd.concat(df_list, ignore_index=True)
    #     # logging.info(concatenated_df.columns)
    #     return concatenated_df

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
