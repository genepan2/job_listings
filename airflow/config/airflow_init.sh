#!/bin/bash

# Install Python modules
# pip install -r /opt/airflow/requirements.txt

# Create some folders (modify these as needed)
# mkdir -p /opt/airflow/data/

airflow variables import /opt/airflow/config/airflow_vars.json
airflow connections import /opt/airflow/config/airflow_conns.json

airflow version
