import os
import re
import json
import logging
import socket
from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, IntegerType
import pyspark.sql.functions as F

# from constants import PATH, JOB_LOCATIONS, JOB_LEVELS, COLLECTIONS, FIELDS
from job_constants import PATH, FIELDS, JOB_LOCATIONS, JOB_LEVELS
import job_helper_transform as JobHelperTransform
from job_helper_storage import merge_recent_files_to_df, save_to_delta
from job_helper_utils import create_key_name, get_spark_session

AWS_SPARK_ACCESS_KEY = os.getenv('MINIO_SPARK_ACCESS_KEY')
AWS_SPARK_SECRET_KEY = os.getenv('MINIO_SPARK_SECRET_KEY')
MINIO_IP_ADDRESS = socket.gethostbyname("minio")


class JobTransformerLinkedIn:
    def __init__(self):
        self.directory_path = os.path.join(PATH['data_raw'], 'linkedin_json')
        self.processed_directory_path = os.path.join(
            PATH['data_processed'], 'linkedin_json')

    # def flatten(self, lst):
    #     flat_list = []
    #     for item in lst:
    #         if isinstance(item, list):
    #             flat_list.extend(self.flatten(item))
    #         else:
    #             flat_list.append(item)
    #     return flat_list

    # def print_json(self, data):
    #     formatted_json = json.dumps(data, indent=4)
    #     logging.info(formatted_json)

    def load_recent_files(self, bucket_name, source_name):
        # Function to load recent files from S3 and return a DataFrame
        return merge_recent_files_to_df(bucket_name, source_name)

    # def load(self):
    #     dir_path = self.directory_path
    #     json_files = [f for f in os.listdir(dir_path) if f.endswith('.json')]
    #     all_data = []
    #     for json_file in json_files:
    #         with open(os.path.join(dir_path, json_file), 'r') as file:
    #             data = json.load(file)
    #             all_data.append(data)
    #     all_data = self.flatten(all_data)
    #     return all_data

    # def transform_old(self, data):
    #     cleaned_data = []
    #     for job in data:
    #         cleaned_job = {key: value.strip() if isinstance(
    #             value, str) else value for key, value in job.items()}

    #         cleaned_job[FIELDS["title"]] = JobHelperTransform.transform_job_title(
    #             cleaned_job[FIELDS["title"]]) if cleaned_job[FIELDS["title"]] else None
    #         cleaned_job[FIELDS["level"]] = JobHelperTransform.transform_job_level(
    #             cleaned_job[FIELDS["level"]], cleaned_job[FIELDS["title"]]) if cleaned_job[FIELDS["level"]] else JOB_LEVELS["middle"]
    #         cleaned_job[FIELDS["location"]] = JobHelperTransform.transform_job_location(
    #             cleaned_job[FIELDS["location"]]) if cleaned_job[FIELDS["location"]] else JOB_LOCATIONS["other"]
    #         cleaned_job[FIELDS["publish_date"]] = JobHelperTransform.transform_to_isoformat(
    #             cleaned_job[FIELDS["publish_date"]], cleaned_job[FIELDS["search_datetime"]])

    #         amount_applicants = re.compile(
    #             r'\d+').findall(cleaned_job[FIELDS["applicants"]]) if cleaned_job[FIELDS["applicants"]] else [0]
    #         cleaned_job[FIELDS["applicants"]] = amount_applicants[0]

    #         cleaned_job[FIELDS["linkedin_id"]] = cleaned_job[FIELDS["linkedin_id"]].replace(
    #             '<!--', '').replace('-->', '') if cleaned_job[FIELDS["linkedin_id"]] else None
    #         cleaned_job[FIELDS["company_linkedin_url"]] = cleaned_job[FIELDS["company_linkedin_url"]].split(
    #             '?')[0] if cleaned_job[FIELDS["company_linkedin_url"]] else None

    #         cleaned_job["language"] = JobHelperTransform.transform_detect_language(
    #             cleaned_job[FIELDS["description"]])

    #         cleaned_data.append(cleaned_job)
    #     return cleaned_data

    def transform_job_level(level, title):
        # Include your existing logic here and return MIDDLE_LEVEL if level is None
        return JobHelperTransform.transform_job_level(level, title) if level else JOB_LEVELS["middle"]

    def clean_linkedin_id(linkedin_id):
        return linkedin_id.replace('<!--', '').replace('-->', '') if linkedin_id else None

    def clean_company_linkedin_url(company_url):
        return company_url.split('?')[0] if company_url else None

    def transform(self, df):

        # clean_string_udf = udf(JobHelperTransform.clean_string, StringType())
        transform_job_title_udf = udf(
            JobHelperTransform.transform_job_title, StringType())
        # transform_job_level_udf = udf(JobHelperTransform.transform_job_level, StringType())
        transform_job_location_udf = udf(
            JobHelperTransform.transform_job_location, StringType())
        transform_to_isoformat_udf = udf(
            JobHelperTransform.transform_to_isoformat, StringType())
        transform_detect_language_udf = udf(
            JobHelperTransform.transform_detect_language, StringType())
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

    # def clean_filename(self, string, replace=False):
    #     pattern = "[,!.\-: ]"
    #     if replace == False:
    #         filename = re.sub(pattern, "_", string)
    #     else:
    #         filename = re.sub(pattern, "", string)
    #     return filename.lower().replace("__", "_")

    # def create_file_name(self, isRaw=False, type="json"):
    #     path = self.directory_path if isRaw else self.processed_directory_path
    #     return f"{path}/linkedin_cleaned_data.json"

    # def save_jobs(self, data, type="json"):
    #     file_name = self.create_file_name(isRaw=False)
    #     with open(file_name, "w") as json_file:
    #         json.dump(data, json_file, indent=4)

    def run_all(self):
        # data_raw = self.load()
        # data_clean = self.transform(data_raw)
        # self.save_jobs(data_clean)
        spark = get_spark_session(
            'LinkedInTransformer', f"http://{MINIO_IP_ADDRESS}:9000", AWS_SPARK_ACCESS_KEY, AWS_SPARK_SECRET_KEY)
        data_raw = self.load_recent_files('bronze', 'linkedin')
        data_clean = self.transform(data_raw)
        # target_path = create_key_name('linkedin', True)
        # target_path = "s3a://my-datalake-bucket/silver/linkedin_data"
        save_to_delta(data_clean, "s3a://silver/linkedin_data")
        spark.stop()
