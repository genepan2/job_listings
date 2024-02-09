from spark_session_manager import SparkSessionManager
from s3_client_manager import S3ClientManager
from file_processing import FileProcessing
from data_storage import DataStorage
from data_transformation import DataTransformation
from data_enrichment import DataEnrichment

from pyspark.sql.functions import col
from pyspark.sql import DataFrame
import os
import yaml

# from airflow.exceptions import AirflowFailException
import logging
import sys
import socket

import re


def load_config(config_part):
    dir_path = os.path.dirname(os.path.abspath(__file__))
    config_file_path = os.path.join(dir_path, 'config.yaml')
    with open(config_file_path, 'r') as file:
        config = yaml.safe_load(file)

    return config[config_part]


def to_snake_case(name):
    # Zuerst von CamelCase/PascalCase zu snake_case
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower() + "_df"


def is_dataframe_empty(df: DataFrame) -> bool:
    """Check if a DataFrame is empty."""
    return df.count() == 0


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
    data_enrichment = DataEnrichment(spark_session)

    # Der Hauptablauf würde hier folgen, z.B.:
    data_raw = file_processor.load_recent_files()

    schema = data_transformation.get_df_schema(source_name)
    spark_df = spark_session.createDataFrame(data_raw, schema=schema)

    data_clean_df = data_transformation.transform(spark_df)

    # Create the Dim DataFrames
    # index,company_name,company_linkedin_url,title,location,linkedin_id,url,applicants,publish_date,level,employment,function,industries,description,search_datetime,search_keyword,search_location,fingerprint,language
    dataframes = {
        "dim_jobs_df": data_clean_df.select(
            "title", "title_cleaned", "description", "source_identifier", "fingerprint"),
        "dim_locations_df": data_clean_df.select(
            col("location").alias("city"), "country"),  # country is not there yet!
        "dim_languages_df": data_clean_df.select(col("language").alias("name")),
        "dim_sources_df": data_clean_df.select(
            col("source").alias("name")),  # source is not there
        "dim_jobLevels_df": data_clean_df.select(col("level").alias("name")),
        "dim_searchKeywords_df": data_clean_df.select(
            col("search_keyword").alias("name")),
        "dim_searchLocations_df": data_clean_df.select(
            col("search_location").alias("name")),
        "dim_dates_df": data_clean_df.select(),
        "dim_employments_df": data_clean_df.select(col("employment").alias("name")),
        "dim_industries_df": data_clean_df.select(col("industries").alias("name")),
        # "dim_skillCategory_df": data_clean_df.select(),
        # "dim_technologyCategory_df": data_clean_df.select(),
        # "dim_skills_df": data_clean_df.select(),
        # "dim_technologies_df": data_clean_df.select(),
        "dim_company_df": data_clean_df.select(col("company_name").alias("name"))
    }

    fact_df = data_clean_df.select("company_name", "title", "location", "job_apps_count", "level", "employment",
                                   "industries", "search_datetime", "search_keyword", "search_location", "fingerprint", "language", "scrape_dur_ms")

    # load config
    dimensions_info = load_config("dimensions_info")

    # Loop through each dimension table and perform the enrichment
    for dim_table, info in dimensions_info.items():
        # Determine the DataFrame name based on the key
        dataframe_name = to_snake_case(dim_table)

        # Determine the dimIdColumn if not provided or empty
        if not info.get("dimIdColumn"):
            info["dimIdColumn"] = data_enrichment.generate_dim_id_column_name(
                dim_table)

        # Access the corresponding DataFrame
        dim_df = dataframes.get(dataframe_name)
        if not dim_df:
            raise ValueError(f"DataFrame {dataframe_name} not found")

        # Load existing data from the dimension table
        dim_existing_df = data_enrichment.load_dimension_table(dim_table)

        # Identify new values by comparing with existing data
        dim_new_values_df = dim_df.select(
            info.get("distinctColumns")).distinct().exceptAll(dim_existing_df)

        # # If there are new values, save them to the dimension table
        # if not is_dataframe_empty(dim_new_values_df):
        #     data_enrichment.save_dimension_table(dim_new_values_df, dim_table)
        #     # Reload the dimension table to include the newly added values
        #     dim_existing_df = data_enrichment.load_dimension_table(dim_table)

        # # Perform the join to enrich the fact dataframe
        # fact_df = fact_df.join(dim_existing_df, fact_df[info["factColumn"]] == dim_existing_df[info["dimColumn"]], "left") \
        #     .withColumn(info["factForeignKeyColumn"], dim_existing_df[info["dimIdColumn"]])

        # TODO: this needs to iterate for saving the dim tables and separately the fact table
        # target_path_delta = f"s3a://{bucket_to}/{source_name}_data"
        # target_path_csv = f"s3a://{bucket_to}/{source_name}_data_csv"
        target_path_delta = f"s3a://{bucket_to}/{source_name}/delta/{dim_table}"
        target_path_csv = f"s3a://{bucket_to}/{source_name}/csv/{dim_table}"

        data_storage.save_to_delta(data_clean_df, target_path_delta)
        data_storage.save_as_csv(data_clean_df, target_path_csv)

    spark_manager.stop()
