from airflow import DAG
from airflow.decorators import task
# from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable

# from airflow.providers.mongo.hooks.mongo import MongoHook
# from airflow.models.baseoperator import chain, cross_downstream
# from airflow.exceptions import AirflowException

from datetime import datetime, timedelta
import pendulum
import json
# import logging


from common.JobListings.ExtractorLinkedIn import ExtractorLinkedIn as Extractor
from common.JobListings.TransformerLinkedIn import TransformerLinkedIn as Transoformer
from common.JobListings.SalaryPredictor import SalaryPredictor
import common.JobListings.HelperDatabase as HelperDatabase
import common.JobListings.HelperUtils as HelperUtils


from common.JobListings.constants import COLLECTIONS

SOURCE_NAME = "linkedin"

####################################################################################
####################################################################################
# Airflow Variable
####################################################################################
####################################################################################

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


####################################################################################
####################################################################################
# Tasks Methods
####################################################################################
####################################################################################

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
    dag_id="LinkedIn_ETL_ML_Pipeline_bidaily_v02",
    description='Aggregate Job Postings from LinkedIn Platform',
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
                scraper = Extractor(keyword, location, JOBS_TO_GET)
                scraper.scrape_jobs()
    extract = extract_linkedin_jobs()

    @task(task_id="transform_linkedin")
    def transform_linkedin_jobs():
        transformer = Transoformer()
        transformer.run_all()
    transform = transform_linkedin_jobs()

    @task(task_id="load_linkedin")
    def load_linkedin_to_mongodb():
        file_path = HelperUtils.construct_file_path_for_data_source(
            SOURCE_NAME)
        HelperDatabase.load_data_to_collection(SOURCE_NAME, file_path)
    load_temp = load_linkedin_to_mongodb()

    @task(task_id="predict_salary_linkedin")
    def ml_predict_salary():
        predictor = SalaryPredictor(SOURCE_NAME)
        predictor.predict_and_map_salaries()
    predict_salary = ml_predict_salary()

    @task(task_id="load_data_to_main")
    def load_linkedin_to_main_collection():
        HelperDatabase.load_records_to_main_collection(SOURCE_NAME)
    load_main = load_linkedin_to_main_collection()

    cleanup_raw = BashOperator(
        task_id='archive_raw_linkedin_files',
        bash_command=move_raw_json_files_to_archive,
    )

    extract >> transform >> load_temp >> predict_salary >> load_main >> cleanup_raw