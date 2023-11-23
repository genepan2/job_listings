from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.models.baseoperator import chain, cross_downstream
from airflow.exceptions import AirflowException

from pymongo import MongoClient
import pymongo
import os
import json
import logging


COLLECTIONS = {
  "themuse": "jobs_themuse",
  "whatjobs": "jobs_whatjobs",
  "linkedin": "jobs_linkedin",
  # "test": "test",
  "all": "jobs_jobs"
}

MONGO = {
  "uri": "mongodb://mongo:27017/",
  "db": "job_listing_db",
  "conn_id": "jobs_mongodb"
}

default_args = {
    'owner': 'you',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    dag_id='test_empty_collections',
    tags=["jobs", "project", "test"],
    default_args=default_args,
    description='for testing',
    # schedule_interval=timedelta(days=1),  # This means the DAG will run daily. Adjust as needed.
    # schedule_interval=timedelta(hours=4),  # This means the DAG will run every 4 hour
    schedule_interval=None,  # This means the DAG will run daily. Adjust as needed.
    start_date=datetime(2023, 10, 1),  # Adjust your start date accordingly
    catchup=False,
)


def delete_all_data_from_collections():
    mongo = MongoHook(conn_id=MONGO['conn_id'])

    for source, collection_name in COLLECTIONS.items():
      collection = mongo.get_collection(collection_name, MONGO["db"])
      try:
        collection.delete_many({})
      except Exception as e:
          logging.info(f"An error occurred while deleting to the database: {e}")


t1 = PythonOperator(
    task_id='test_empty_all_collections',
    python_callable=delete_all_data_from_collections,
    dag=dag,
)


t1


if __name__ == "__main__":
    dag.cli()