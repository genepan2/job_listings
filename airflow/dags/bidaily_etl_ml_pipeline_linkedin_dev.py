from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash_operator import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.hooks.postgres_hook import PostgresHook
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

from common.JobListings.extractor_linkedin import ExtractorLinkedIn as Extractor
# from common.JobListings.transformer_linkedin import TransformerLinkedIn as Transoformer
from common.JobListings.predictor_salary import PredictorSalary
import common.JobListings.helper_database as HelperDatabase
import common.JobListings.helper_utils as HelperUtils

SOURCE_NAME = "linkedin"
BUCKET_FROM = 'bronze'
BUCKET_TO = 'silver'
DELTA_MINUTES = 600

AWS_SPARK_ACCESS_KEY = os.getenv('MINIO_SPARK_ACCESS_KEY')
AWS_SPARK_SECRET_KEY = os.getenv('MINIO_SPARK_SECRET_KEY')
SPARK_HISTORY_LOG_DIR = os.getenv('SPARK_HISTORY_LOG_DIR')
# not sure whether this is realy necassery
MINIO_IP_ADDRESS = socket.gethostbyname("minio")

keywords_linkedin = json.loads(Variable.get("search_keyword_linkedin"))
locations_linkedin = json.loads(Variable.get("search_location_linkedin"))

jobs_to_load = Variable.get("jobs_to_load", default_var=None)


if jobs_to_load == 'None' or not jobs_to_load:
    JOBS_TO_GET = None
else:
    try:
        JOBS_TO_GET = int(jobs_to_load)
    except ValueError:
        raise ValueError(
            f"Expected 'jobs_to_load' to be an integer or 'None', got: {jobs_to_load}")

move_raw_json_files_to_archive = '''
current_time=$(date "+%Y-%m-%d_%H-%M-%S")
archive_dir="/opt/airflow/data/archive/${current_time}"
mkdir -p "${archive_dir}"
find /opt/airflow/data/raw/linkedin_json -type f -name '*.json' -exec mv {} "${archive_dir}" \;
'''

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id="LinkedIn_ETL_ML_Pipeline_DEV",
    description='Aggregate Job Postings from LinkedIn Platform',
    tags=["jobs", "project"],
    start_date=pendulum.datetime(2023, 10, 1, tz="UTC"),
    schedule_interval=timedelta(hours=2),
    schedule=None,
    default_args=default_args,
    catchup=False,
) as dag:

    # @task(task_id="extract_linkedin")
    # def extract_linkedin_jobs():
    #     for keyword in keywords_linkedin:
    #         for location in locations_linkedin:
    #             scraper = Extractor(keyword, location, JOBS_TO_GET)
    #             scraper.scrape_jobs()
    # extract = extract_linkedin_jobs()

    # this is only temporary. to test if saving as delta works.
    # @task(task_id="extract_linkedin")
    # def extract_linkedin_jobs():
    #     scraper = Extractor(keywords_linkedin, locations_linkedin, JOBS_TO_GET)
    #     scraper.scrape_jobs()
    # extract = extract_linkedin_jobs()

    # @task(task_id="transform_linkedin")
    # def transform_linkedin_jobs():
    #     transformer = Transoformer()
    #     transformer.run_all()
    # transform = transform_linkedin_jobs()

    @task(task_id="fetch_and_store_data_from_dw")
    def fetch_and_store_data_from_dw():
        # PostgreSQL Connection
        # this connection was created via env variable in docker compose
        pg_hook = PostgresHook(postgres_conn_id="postgres_jobs")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # TODO: this could be done with a loop
        # Get Data from PostgreSQL
        query_locations = "SELECT * FROM dimLocations;"
        query_sources = "SELECT * FROM dimSources;"
        df_locations = pd.read_sql_query(query_locations, conn)
        df_sources = pd.read_sql_query(query_sources, conn)

        # Transofrm Data to CSV
        csv_buffer = StringIO()
        df_locations.to_csv(csv_buffer, index=False,
                            quoting=csv.QUOTE_NONNUMERIC)

        # Save CSV in S3
        # s3_resource = boto3.resource('s3')
        # s3_resource.Object('your_bucket_name', 'dimLocations.csv').put(Body=csv_buffer.getvalue())
    fetch_store_data = fetch_and_store_data_from_dw()

    transform_spark = SparkSubmitOperator(
        task_id=f"transform_{SOURCE_NAME}_spark",
        conn_id='jobs_spark_conn',
        application='./dags/common/JobListings/spark/transform_jobs.py',
        py_files='./dags/common/JobListings/spark/helper_transform.py,./dags/common/JobListings/spark/constants.py',
        # not sure about the "mariadb-java-client-3.3.2.jar"
        jars='./dags/jars/mariadb-java-client-3.3.2.jar,./dags/jars/aws-java-sdk-bundle-1.12.262.jar,./dags/jars/delta-spark_2.12-3.0.0.jar,./dags/jars/delta-storage-3.0.0.jar,./dags/jars/hadoop-aws-3.3.4.jar,./dags/jars/hadoop-common-3.3.4.jar',
        application_args=[f"{SOURCE_NAME}Transformer",
                          SOURCE_NAME, BUCKET_FROM, BUCKET_TO, str(DELTA_MINUTES)],
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
            "spark.delta.logStore.class": "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
            # todo: tuning configurations -> research
            # "spark.sql.shuffle.partitions": 200
        }
        # executor_memory='2g',
        # executor_cores=2,
        # driver_memory='2g',
    )

    # extract >> transform >> load_temp >> predict_salary >> load_main >> cleanup_raw
    # extract
    # extract >> transform_spark
    transform_spark
