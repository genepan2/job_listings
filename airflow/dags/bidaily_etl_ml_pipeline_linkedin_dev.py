from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash_operator import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from datetime import timedelta
from io import StringIO
import pendulum
import json
import os
import socket
import pandas as pd
import csv
import boto3
import logging
import yaml

from common.JobListings.job_extractor_linkedin import JobExtractorLinkedIn

# from common.JobListings.job_transformer_linkedin import JobTransformerLinkedIn
# from common.JobListings.job_predictor_salary import JobPredictorSalary
# import common.JobListings.job_helper_database as JobHelperDatabase
# import common.JobListings.job_helper_utils as JobHelperUtils
from common.JobListings.job_data_loader import JobDataLoader


SOURCE_NAME = "linkedin"
BUCKET_FROM = "bronze"
BUCKET_TO = "silver"
# DELTA_MINUTES = (60 * 24 * 2) + 300
DELTA_MINUTES = 240

AWS_SPARK_ACCESS_KEY = os.getenv("MINIO_SPARK_ACCESS_KEY")
AWS_SPARK_SECRET_KEY = os.getenv("MINIO_SPARK_SECRET_KEY")
SPARK_HISTORY_LOG_DIR = os.getenv("SPARK_HISTORY_LOG_DIR")
# not sure whether this is realy necassery
MINIO_IP_ADDRESS = socket.gethostbyname("minio")

keywords_linkedin = json.loads(Variable.get("search_keyword_linkedin"))
locations_linkedin = json.loads(Variable.get("search_location_linkedin"))

jobs_to_load = Variable.get("jobs_to_load", default_var=None)


if jobs_to_load == "None" or not jobs_to_load:
    JOBS_TO_GET = None
else:
    try:
        JOBS_TO_GET = int(jobs_to_load)
    except ValueError:
        raise ValueError(
            f"Expected 'jobs_to_load' to be an integer or 'None', got: {jobs_to_load}"
        )

# move_raw_json_files_to_archive = '''
# current_time=$(date "+%Y-%m-%d_%H-%M-%S")
# archive_dir="/opt/airflow/data/archive/${current_time}"
# mkdir -p "${archive_dir}"
# find /opt/airflow/data/raw/linkedin_json -type f -name '*.json' -exec mv {} "${archive_dir}" \;
# '''

default_args = {
    "owner": "admin",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="LinkedIn_ETL_ML_Pipeline_DEV",
    description="Aggregate Job Postings from LinkedIn Platform",
    tags=["jobs", "project"],
    start_date=pendulum.datetime(2023, 10, 1, tz="UTC"),
    schedule_interval=timedelta(hours=2),
    schedule=None,
    default_args=default_args,
    catchup=False,
) as dag:

    @task(task_id="extract_linkedin")
    def extract_linkedin_jobs():
        for keyword in keywords_linkedin:
            for location in locations_linkedin:
                scraper = JobExtractorLinkedIn(keyword, location, JOBS_TO_GET)
                scraper.scrape_jobs()

    extract = extract_linkedin_jobs()

    # this is only temporary. to test if saving as delta works.
    # @task(task_id="extract_linkedin")
    # def extract_linkedin_jobs():
    #     scraper = JobExtractorLinkedIn(keywords_linkedin, locations_linkedin, JOBS_TO_GET)
    #     scraper.scrape_jobs()
    # extract = extract_linkedin_jobs()

    # @task(task_id="transform_linkedin")
    # def transform_linkedin_jobs():
    #     transformer = JobTransoformer()
    #     transformer.run_all()
    # transform = transform_linkedin_jobs()

    # @task(task_id="fetch_and_store_data_from_dw")
    # def fetch_and_store_data_from_dw():
    #     # PostgreSQL Connection
    #     # this connection was created via env variable in docker compose
    #     pg_hook = PostgresHook(postgres_conn_id="postgres_jobs")
    #     conn = pg_hook.get_conn()

    #     # S3 Connection
    #     s3_hook = S3Hook(aws_conn_id='S3_conn')

    #     table_names = ['dimLocations', 'dimLanguages', 'dimSources', 'dimJobLevels', 'dimSearchKeywords',
    #                    'dimEmployments', 'dimIndustries', 'dimSkillCategory', 'dimTechnologyCategory', 'dimSkills', 'dimTechnologies']

    #     for table_name in table_names:
    #         logging.info(f"Getting Data from ${table_name}")
    #         # get data
    #         query = f"SELECT * FROM {table_name};"
    #         df = pd.read_sql_query(query, conn)

    #         # transform to csv
    #         csv_buffer = StringIO()
    #         df.to_csv(csv_buffer, index=False, quoting=csv.QUOTE_NONNUMERIC)

    #         # saveas csv
    #         logging.info(f"Saving Data from ${table_name}")
    #         s3_hook.load_string(
    #             string_data=csv_buffer.getvalue(),
    #             bucket_name='silver',
    #             key=f'dimTables/{table_name}.csv',
    #             replace=True
    #         )
    #         logging.info(f"Done with ${table_name}")
    # fetch_store_data = fetch_and_store_data_from_dw()

    # source_file = '${AIRFLOW_HOME}/dags/common/JobListings/constants.py'
    # destination_file = '${AIRFLOW_HOME}/dags/common/JobListings/spark/constants.py'

    # # Copy the constants to spark folder before running spark
    # copy_constants = BashOperator(
    #     task_id='copy_constants',
    #     bash_command=f'cp -f {source_file} {destination_file}',
    #     dag=dag,
    # )

    # TODO: maybe better to just get all files from folder...
    spark_folder_path = "./dags/common/JobListings/"
    spark_py_files = [
        # "__init__.py",
        "job_config_manager.py",
        "job_config_constants.py",
        "job_data_enrichment.py",
        "job_data_storage.py",
        "job_data_transformation.py",
        "job_file_processing.py",
        "job_helper_transform.py",
        "job_s3_client_manager.py",
        "job_spark_session_manager.py",
    ]

    extended_py_files = ",".join([spark_folder_path + file for file in spark_py_files])

    spark_jars_folder = "./dags/jars/"
    spark_jar_files = [
        "mariadb-java-client-3.3.2.jar",  # not sure about this one...
        "aws-java-sdk-bundle-1.12.262.jar",
        "delta-spark_2.12-3.0.0.jar",
        "delta-storage-3.0.0.jar",
        "hadoop-aws-3.3.4.jar",
        "hadoop-common-3.3.4.jar",
        "postgresql-42.7.1.jar",
    ]

    extended_jar_files = ",".join(
        [spark_jars_folder + file for file in spark_jar_files]
    )

    transform_spark = SparkSubmitOperator(
        task_id=f"transform_{SOURCE_NAME}_spark",
        conn_id="jobs_spark_conn",
        application="./dags/common/JobListings/job_spark_transform.py",
        py_files=extended_py_files,
        jars=extended_jar_files,
        application_args=[
            f"{SOURCE_NAME}Transformer",
            SOURCE_NAME,
            BUCKET_FROM,
            BUCKET_TO,
            str(DELTA_MINUTES),
        ],
        conf={
            "spark.network.timeout": "10000s",
            #  "hive.metastore.uris": hive_metastore,
            #  "hive.exec.dynamic.partition": "true",
            #  "hive.exec.dynamic.partition.mode": "nonstrict",
            "spark.sql.sources.partitionOverwriteMode": "dynamic",
            "spark.hadoop.fs.s3a.multiobjectdelete.enable": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.fast.upload": "true",
            "spark.hadoop.fs.s3a.endpoint": f"http://{MINIO_IP_ADDRESS}:9000",
            "spark.hadoop.fs.s3a.access.key": AWS_SPARK_ACCESS_KEY,
            "spark.hadoop.fs.s3a.secret.key": AWS_SPARK_SECRET_KEY,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.history.fs.logDirectory": f"s3a://{SPARK_HISTORY_LOG_DIR}/",
            "spark.sql.files.ignoreMissingFiles": "true",
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            # "spark.databricks.delta.schema.autoMerge.enabled": "true",
            "spark.delta.logStore.class": "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
            "spark.executorEnv.PYTHONPATH": os.getenv("PYTHONPATH"),
            # todo: tuning configurations -> research
            # "spark.sql.shuffle.partitions": 200
        },
        # executor_memory="2g",
        # executor_cores=2,
        # driver_memory="2g",
    )

    @task(task_id=f"load_{SOURCE_NAME}_data")
    def load_linkedin_jobs():
        data_loader = JobDataLoader(SOURCE_NAME, BUCKET_TO, "job_config_tables.yaml")
        data_loader.main()

    load = load_linkedin_jobs()

    # Delete the constants file after running spark
    # delete_constants = BashOperator(
    #     task_id='delete_constants',
    #     bash_command=f'rm {destination_file}',
    #     dag=dag,
    # )

    # extract >> transform >> load_temp >> predict_salary >> load_main >> cleanup_raw
    # extract
    # fetch_store_data
    # transform_spark
    # extract >> fetch_store_data >> copy_constants >> transform_spark >> delete_constants
    # extract >> fetch_store_data >> copy_constants >> transform_spark
    # transform_spark
    # extract >> copy_constants >> transform_spark
    extract >> transform_spark >> load
    # transform_spark >> load
