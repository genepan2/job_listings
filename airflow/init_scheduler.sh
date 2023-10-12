#!/bin/bash

# Install Python modules
pip install -r /opt/airflow/requirements.txt

# Create some folders (modify these as needed)
# mkdir -p /opt/airflow/data/

# Import Airflow variables
airflow variables import /opt/airflow/variables.json

# Check Airflow version
# airflow scheduler
airflow version
