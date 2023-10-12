import json
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import logging

# Define the default arguments
default_args = {
    'owner': 'you',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    dag_id='write_json_test_two',
    tags=["jobs", "test"],
    default_args=default_args,
    description='Test DAG to write a JSON file',
    schedule_interval=None,  # This DAG is manually triggered, it won't run on a schedule
    start_date=datetime(2023, 10, 10),
    catchup=False,
)

# Define the directory and file path
dir_path = "/opt/airflow/data/"
file_path = os.path.join(dir_path, "test.json")

# Define the Python function to write the JSON file
def write_json_file():
    # Check if directory exists, if not create it
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    data = {
        "name": "John Doe",
        "age": 30,
        "city": "New York"
    }
    with open(file_path, "w") as file:
        json.dump(data, file)
    logging.info(f"JSON file written successfully to {file_path}!")

# Define the task that uses the Python function
t1 = PythonOperator(
    task_id='write_json',
    python_callable=write_json_file,
    dag=dag,
)

if __name__ == "__main__":
    dag.cli()