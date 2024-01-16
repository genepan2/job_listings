from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash_operator import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
from datetime import timedelta
import pendulum
import json
import os
import socket

from common.JobListings.ExtractorLinkedIn import ExtractorLinkedIn as Extractor
from common.JobListings.TransformerLinkedIn import TransformerLinkedIn as Transoformer
from common.JobListings.SalaryPredictor import SalaryPredictor
import common.JobListings.HelperDatabase as HelperDatabase
import common.JobListings.HelperUtils as HelperUtils

SOURCE_NAME = "linkedin"
AWS_SPARK_ACCESS_KEY = os.getenv('MINIO_SPARK_ACCESS_KEY')
AWS_SPARK_SECRET_KEY = os.getenv('MINIO_SPARK_SECRET_KEY')
SPARK_HISTORY_LOG_DIR = os.getenv('SPARK_HISTORY_LOG_DIR')
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

    transform_spark = SparkSubmitOperator(
        task_id='transform_linkedin_spark',
        conn_id='spark_dev',  # Connection to Spark cluster
        # application='/home/ubuntu/jobs_listings/transform_jobs.py', #this is pointing to the spark container (not working)
        application='./dags/common/JobListings/spark/transform_jobs.py',
        py_files='./dags/common/JobListings/spark/helper_transform.py,./dags/common/JobListings/spark/constants.py',
        # jars='./dags/jars/aws-java-sdk-1.11.534.jar,./dags/jars/aws-java-sdk-bundle-1.11.901.jar,./dags/jars/delta-core_2.12-2.2.0.jar,./dags/jars/mariadb-java-client-3.1.4.jar,./dags/jars/hadoop-aws-3.3.4.jar',
        # jars='./dags/jars/aws-java-sdk-1.12.610.jar,./dags/jars/aws-java-sdk-bundle-1.12.262.jar,./dags/jars/delta-core_2.12-2.4.0.jar,./dags/jars/mariadb-java-client-3.3.0.jar,./dags/jars/hadoop-aws-3.3.4.jar',
        # jars='./dags/jars/aws-java-sdk-1.12.610.jar,./dags/jars/aws-java-sdk-bundle-1.12.262.jar,./dags/jars/delta-core_2.13-2.3.0.jar,./dags/jars/mariadb-java-client-3.3.0.jar,./dags/jars/hadoop-aws-3.3.4.jar',
        # jars='./dags/jars/aws-java-sdk-1.12.610.jar,./dags/jars/aws-java-sdk-bundle-1.12.262.jar,./dags/jars/delta-core_2.13-2.3.0.jar,./dags/jars/mariadb-java-client-3.3.0.jar,./dags/jars/hadoop-aws-3.3.2.jar',
        # jars='./dags/jars/aws-java-sdk-1.12.610.jar,./dags/jars/aws-java-sdk-bundle-1.12.262.jar,./dags/jars/delta-core_2.12-2.4.0.jar,./dags/jars/mariadb-java-client-3.3.0.jar,./dags/jars/hadoop-aws-3.3.2.jar',
        # jars='./dags/jars/aws-java-sdk-1.12.610.jar,./dags/jars/aws-java-sdk-bundle-1.12.262.jar,./dags/jars/delta-core_2.13-2.2.0.jar,./dags/jars/mariadb-java-client-3.3.0.jar,./dags/jars/hadoop-aws-3.3.2.jar',
        # jars='./dags/jars/aws-java-sdk-1.12.610.jar,./dags/jars/aws-java-sdk-bundle-1.12.262.jar,./dags/jars/delta-core_2.12-2.2.0.jar,./dags/jars/mariadb-java-client-3.3.0.jar,./dags/jars/hadoop-aws-3.3.2.jar',
        # jars='./dags/jars/aws-java-sdk-1.12.610.jar,./dags/jars/aws-java-sdk-bundle-1.12.262.jar,./dags/jars/delta-core_2.12-2.4.0.jar,./dags/jars/mariadb-java-client-3.3.0.jar,./dags/jars/hadoop-aws-3.3.2.jar',
        # jars='./dags/jars/aws-java-sdk-1.12.262.jar,./dags/jars/aws-java-sdk-bundle-1.12.262.jar,./dags/jars/delta-core_2.12-2.4.0.jar,./dags/jars/mariadb-java-client-3.3.0.jar,./dags/jars/hadoop-aws-3.3.4.jar,./dags/jars/delta-storage-2.4.0.jar',
        # jars='./dags/jars/delta-core_2.12-2.4.0.jar,./dags/jars/mariadb-java-client-3.3.0.jar,./dags/jars/hadoop-aws-3.3.4.jar',
        # jars='./dags/jars/aws-java-sdk-bundle-1.12.262.jar,./dags/jars/delta-core_2.12-2.4.0.jar,./dags/jars/mariadb-java-client-3.3.0.jar,./dags/jars/hadoop-aws-3.3.4.jar,./dags/jars/delta-storage-2.4.0.jar',
        # jars='./dags/jars/mariadb-java-client-3.3.2.jar,./dags/jars/aws-java-sdk-bundle-1.12.634.jar,./dags/jars/delta-core_2.13-2.4.0.jar,./dags/jars/delta-storage-3.0.0.jar,./dags/jars/hadoop-aws-3.3.6.jar',
        # jars='./dags/jars/mariadb-java-client-3.3.2.jar,./dags/jars/aws-java-sdk-bundle-1.12.634.jar,./dags/jars/delta-iceberg_2.12-3.0.0.jar,./dags/jars/hadoop-aws-3.3.6.jar',
        # jars='./dags/jars/mariadb-java-client-3.3.2.jar,./dags/jars/aws-java-sdk-bundle-1.12.367.jar,./dags/jars/delta-core_2.12-2.4.0.jar,./dags/jars/hadoop-aws-3.3.6.jar,./dags/jars/hadoop-common-3.3.6.jar',
        # jars='./dags/jars/mariadb-java-client-3.3.2.jar,./dags/jars/aws-java-sdk-bundle-1.12.634.jar,./dags/jars/delta-core_2.12-2.4.0.jar,./dags/jars/delta-storage-3.0.0.jar,./dags/jars/hadoop-aws-3.3.6.jar,./dags/jars/hadoop-common-3.3.6.jar',
        # jars='./dags/jars/mariadb-java-client-3.3.2.jar,./dags/jars/aws-java-sdk-bundle-1.12.634.jar,./dags/jars/delta-spark_2.12-3.0.0.jar,./dags/jars/delta-storage-3.0.0.jar,./dags/jars/hadoop-aws-3.3.6.jar,./dags/jars/hadoop-common-3.3.6.jar',
        jars='./dags/jars/mariadb-java-client-3.3.2.jar,./dags/jars/aws-java-sdk-bundle-1.12.262.jar,./dags/jars/delta-spark_2.12-3.0.0.jar,./dags/jars/delta-storage-3.0.0.jar,./dags/jars/hadoop-aws-3.3.4.jar,./dags/jars/hadoop-common-3.3.4.jar',
        # Additional options:
        # Arguments for your application
        application_args=['LinkedInTransformer', 'linkedin', 'silver'],
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
            #  "spark.history.fs.logDirectory": "s3a://spark/",
            "spark.history.fs.logDirectory": f"s3a://{SPARK_HISTORY_LOG_DIR}/",
            "spark.sql.files.ignoreMissingFiles": "true",
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.delta.logStore.class": "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
        }
        # executor_memory='2g',
        # executor_cores=2,
        # driver_memory='2g',
        # ...
        # dag=dag
    )

    # @task(task_id="load_linkedin")
    # def load_linkedin_to_mongodb():
    #     file_path = HelperUtils.construct_file_path_for_data_source(
    #         SOURCE_NAME)
    #     HelperDatabase.load_data_to_collection(SOURCE_NAME, file_path)
    # load_temp = load_linkedin_to_mongodb()

    # @task(task_id="predict_salary_linkedin")
    # def ml_predict_salary():
    #     predictor = SalaryPredictor(SOURCE_NAME)
    #     predictor.predict_and_map_salaries()
    # predict_salary = ml_predict_salary()

    # @task(task_id="load_data_to_main")
    # def load_linkedin_to_main_collection():
    #     HelperDatabase.load_records_to_main_collection(SOURCE_NAME)
    # load_main = load_linkedin_to_main_collection()

    # cleanup_raw = BashOperator(
    #     task_id='archive_raw_linkedin_files',
    #     bash_command=move_raw_json_files_to_archive,
    # )

    # extract >> transform >> load_temp >> predict_salary >> load_main >> cleanup_raw
    # extract
    # extract >> transform
    # extract >> transform_spark
    transform_spark
