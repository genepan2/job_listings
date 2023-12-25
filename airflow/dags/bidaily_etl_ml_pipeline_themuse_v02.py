from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pendulum
import json

# Import Themuse specific classes
from common.JobListings.themuse_extractor import ThemuseDataExtractor as Extractor
from common.JobListings.themuse_transformer import ThemuseDataTransformer as Transformer
from common.JobListings.SalaryPredictor import SalaryPredictor
import common.JobListings.HelperDatabase as HelperDatabase
import common.JobListings.HelperUtils as HelperUtils

from common.JobListings.constants import COLLECTIONS

SOURCE_NAME = "themuse"

# Airflow Variable
keywords_themuse = json.loads(Variable.get("search_keyword_themuse"))
locations_themuse = json.loads(Variable.get("search_location_themuse"))

jobs_to_load = Variable.get("jobs_to_load", default_var=None)

# Determine the number of jobs to get
if jobs_to_load == 'None' or not jobs_to_load:
    JOBS_TO_GET = None
else:
    try:
        JOBS_TO_GET = int(jobs_to_load)
    except ValueError:
        raise ValueError(f"Expected 'jobs_to_load' to be an integer or 'None', got: {jobs_to_load}")

# Archive command
move_raw_json_files_to_archive = '''
current_time=$(date "+%Y-%m-%d_%H-%M-%S")
archive_dir="/opt/airflow/data/archive/${current_time}"
mkdir -p "${archive_dir}"
find /opt/airflow/data/raw/themuse_json -type f -name '*.json' -exec mv {} "${archive_dir}" \;
'''

# DAG default arguments
default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

# DAG definition
with DAG(
    dag_id="TheMuse_ETL_ML_Pipeline_bidaily_v02",
    description='Aggregate Job Postings from The Muse Platform',
    tags=["jobs", "project"],
    start_date=pendulum.datetime(2023, 10, 1, tz="UTC"),
    schedule_interval=timedelta(hours=2),
    default_args=default_args,
    catchup=False,
) as dag:

    @task(task_id="extract_themuse")
    def extract_themuse_jobs():
        for keyword in keywords_themuse:
            for location in locations_themuse:
                extractor = Extractor([keyword], [location], JOBS_TO_GET)
                extractor.extract_jobs()
    extract = extract_themuse_jobs()

    @task(task_id="transform_themuse")
    def transform_themuse_jobs():
        transformer = Transformer()
        transformer.transform_jobs()
    transform = transform_themuse_jobs()

    @task(task_id="load_themuse")
    def load_themuse_to_mongodb():
        file_path = HelperUtils.construct_file_path_for_data_source(SOURCE_NAME)
        HelperDatabase.load_data_to_collection(SOURCE_NAME, file_path)
    load_temp = load_themuse_to_mongodb()

    @task(task_id="predict_salary_themuse")
    def ml_predict_salary():
        predictor = SalaryPredictor(SOURCE_NAME)
        predictor.predict_and_map_salaries()
    predict_salary = ml_predict_salary()

    @task(task_id="load_data_to_main_themuse")
    def load_themuse_to_main_collection():
        HelperDatabase.load_records_to_main_collection(SOURCE_NAME)
    load_main = load_themuse_to_main_collection()

    cleanup_raw = BashOperator(
        task_id='archive_raw_themuse_files',
        bash_command=move_raw_json_files_to_archive,
    )

    extract >> transform >> load_temp >> predict_salary >> load_main >> cleanup_raw
