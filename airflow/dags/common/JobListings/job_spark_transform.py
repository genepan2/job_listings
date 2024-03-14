from job_spark_session_manager import JobSparkSessionManager
from job_s3_client_manager import JobS3ClientManager
from job_file_processing import JobFileProcessing
from job_data_storage import JobDataStorage
from job_data_transformation import JobDataTransformation
from job_data_enrichment import JobDataEnrichment
from job_config_manager import JobConfigManager
from job_helper_utils import generate_id_column_name

from pyspark.sql.functions import col
from pyspark.sql import DataFrame
import os
import yaml
from stringcase import snakecase, camelcase, pascalcase

# from airflow.exceptions import AirflowFailException
import logging
import sys
import socket

import re

DO_DIM_TABLES = True


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

    spark_manager = JobSparkSessionManager(appname, "ALL")
    spark_session = spark_manager.get_spark_session()

    # setup logger
    spark_session.sparkContext.setLogLevel("ERROR")
    log4jLogger = spark_session._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger("LOGGER")
    logger.setLevel(log4jLogger.Level.INFO)

    AWS_SPARK_ACCESS_KEY = os.getenv("MINIO_SPARK_ACCESS_KEY")
    AWS_SPARK_SECRET_KEY = os.getenv("MINIO_SPARK_SECRET_KEY")
    MINIO_IP_ADDRESS = socket.gethostbyname("minio")
    SPARK_MASTER_IP_ADDRESS = socket.gethostbyname("spark-master")

    s3_client_manager = JobS3ClientManager(
        AWS_SPARK_ACCESS_KEY, AWS_SPARK_SECRET_KEY, f"http://{MINIO_IP_ADDRESS}:9000"
    )
    # s3_client = s3_client_manager.get_boto_client()

    file_processor = JobFileProcessing(
        s3_client_manager, bucket_from, source_name, delta_minutes
    )

    data_storage = JobDataStorage()

    data_transformation = JobDataTransformation(args)
    data_enrichment = JobDataEnrichment(spark_session)

    # Der Hauptablauf würde hier folgen, z.B.:
    data_raw = file_processor.merge_files_to_df()

    # logger.info(data_raw.printSchema())
    # logger.info(data_raw.info())
    # logger.info(data_raw.head(5))

    schema = data_transformation.get_df_schema(source_name)
    spark_df = spark_session.createDataFrame(data_raw, schema=schema)

    data_clean_df = data_transformation.transform(spark_df)
    # data_clean_df.show(25)

    # Create the Dim DataFrames
    # index,company_name,company_linkedin_url,title,location,linkedin_id,url,applicants,publish_date,level,employment,function,industries,description,search_datetime,search_keyword,search_location,fingerprint,language

    dates_df = data_transformation.get_unique_date_values_dataframes(data_clean_df)

    dim_dataframes = data_transformation.get_dataframes_from_data(data_clean_df)
    dim_dataframes["dim_dates_df"] = dates_df

    fct_df = data_transformation.select_fct_columns(data_clean_df)

    # load config
    config_manager = JobConfigManager("job_config_tables.yaml")
    dimensions_info = config_manager.load_config("dimensions_info")

    # Loop through each dimension table and perform the enrichment
    if DO_DIM_TABLES:
        for dim_table_name, dim_tabel_info in dimensions_info.items():
            logger.info(f"Start working on {dim_table_name}")

            # Determine the dimIdColumn if not provided or empty
            # if not dim_tabel_info.get("dimIdColumn"):
            #     dim_tabel_info["dimIdColumn"] = generate_id_column_name(dim_table_name)

            uniqueColumns = dim_tabel_info.get("uniqueColumns")
            if not isinstance(uniqueColumns, list):
                uniqueColumns = [uniqueColumns]

            # Access the corresponding DataFrame
            dataframe_name = snakecase(dim_table_name) + "_df"
            dim_df = dim_dataframes.get(dataframe_name)

            if not dim_df:
                raise ValueError(f"DataFrame {dataframe_name} not found")

            # Load existing data from the dimension table
            # dim_existing_df = data_enrichment.load_dimension_table(
            #     dim_table_name, uniqueColumns
            # )
            dim_existing_values = data_enrichment.load_filtered_table(
                dim_table_name,
                uniqueColumns,
                uniqueColumns[0],
                dim_df,
                uniqueColumns[0],
            )

            # Identify new values by comparing with existing data
            # dim_new_values_df = (
            #     dim_df.select(uniqueColumns).distinct().exceptAll(dim_existing_df)
            # )
            dim_new_values_df = (
                dim_df.select(uniqueColumns).distinct().exceptAll(dim_existing_values)
            )

            # save the dim tables
            if not is_dataframe_empty(dim_new_values_df):
                # an dieser Stelle muss ich ein merge machen mit der dim_df über die uniqueColumns
                dim_df_new_full = dim_new_values_df.join(
                    dim_df,
                    # dim_new_values_df[uniqueColumns[0]] == dim_df[uniqueColumns[0]],
                    uniqueColumns[0],
                    "inner",
                )

                dim_df_new_full.show(25)

                logger.info(f"Start saving {dim_table_name} as delta")
                target_path_delta = (
                    f"s3a://{bucket_to}/{source_name}/delta/{dim_table_name}"
                )
                data_storage.save_from_spark_as_delta(
                    dim_df_new_full, target_path_delta
                )

                logger.info(f"Start saving {dim_table_name} as csv")
                target_path_csv = (
                    f"s3a://{bucket_to}/{source_name}/csv/{dim_table_name}"
                )
                data_storage.save_from_spark_as_csv(dim_df_new_full, target_path_csv)

                logger.info(f"Done saving {dim_table_name}")

    # save the fact table
    fct_info = config_manager.load_config("fct_info")
    fct_table_name = config_manager.get_fct_table_name()
    uniqueColumns = fct_info.get("fctJobListings").get("uniqueColumns")
    # print(type(uniqueColumns))
    # raise ValueError("stop here")
    uniqueColumns.append("created_at")

    fct_existing_values = data_enrichment.load_filtered_table(
        fct_table_name,
        uniqueColumns,
        uniqueColumns[0],
        fct_df,
        uniqueColumns[0],
    )
    # logger.info("fct_existing_values:")
    # logger.info(type(fct_df))
    # print(type(fct_existing_values))
    # fct_existing_values.show()

    # logger.info("fct_df:")
    # print(type(fct_df))
    # fct_df.show()

    columns_to_drop = ["search_datetime"]
    fct_df = fct_df.dropDuplicates([uniqueColumns[0]]).drop(*columns_to_drop)

    # spark_manager.stop()

    fct_merged_df = data_transformation.merge_dataframes(
        fct_df, fct_existing_values, uniqueColumns[0]
    )

    if not is_dataframe_empty(fct_merged_df):
        logger.info("Start saving Fact Table")
        # fct_merged_df.show(25)
        target_path_delta = f"s3a://{bucket_to}/{source_name}/delta/fctJobListings"
        target_path_csv = f"s3a://{bucket_to}/{source_name}/csv/fctJobListings"

        data_storage.save_from_spark_as_csv(fct_merged_df, target_path_csv)
        data_storage.save_from_spark_as_delta(fct_merged_df, target_path_delta)
        logger.info("Done saving Fact Table")

    spark_manager.stop()
