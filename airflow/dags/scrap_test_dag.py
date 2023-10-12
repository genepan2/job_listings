import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
from bs4 import BeautifulSoup


# Constants
OUTPUT_DIR = "/opt/airflow/data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "google.html.txt")

def scrape_google():
    # Fetch the content of www.google.com
    response = requests.get("https://www.google.com")
    soup = BeautifulSoup(response.content, 'html.parser')
    import pdb; pdb.set_trace()


    # Ensure the directory exists
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    # Save the HTML content to a .txt file
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as file:
        file.write(str(soup.prettify()))

    print(f"HTML content of www.google.com saved to {OUTPUT_FILE}")

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
    dag_id='scrape_google',
    default_args=default_args,
    tags=["jobs", "test"],
    description='DAG to scrape www.google.com',
    schedule_interval=None,  # This DAG is manually triggered, it won't run on a schedule
    start_date=datetime(2023, 10, 10),
    catchup=False,
)

# Define the task that uses the Python function
t1 = PythonOperator(
    task_id='scrape_and_save',
    python_callable=scrape_google,
    dag=dag,
)

# if __name__ == "__main__":
#     # dag.cli()
#     dag.test()
