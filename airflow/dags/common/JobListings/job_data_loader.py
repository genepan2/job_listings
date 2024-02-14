import psycopg2
import yaml
import os
import socket
import pandas as pd

from job_s3_client_manager import JobS3ClientManager
from job_config_manager import JobConfigManager


class JobDataLoader:
    def __init__(self, source_name, tables_config):
        self.source_name = source_name
        self.config = self.load_config(tables_config)
        self.connection = self.connect_to_db()

    def connect_to_db(self):
        """Stellt eine Verbindung zur PostgreSQL-Datenbank her."""
        return psycopg2.connect(
            dbname=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            # host=os.getenv('postgres')
            host='postgres'
        )

    def read_new_data_from_s3(self, bucket_name, source_name, table_name):

        return data

    def read_table(self, table_name, columns=None):
        """Liest Daten aus einer PostgreSQL-Tabelle."""
        with self.connection.cursor() as cursor:
            if columns:
                query = f"SELECT {', '.join(columns)} FROM {table_name}"
            else:
                query = f"SELECT * FROM {table_name}"
            cursor.execute(query)
            return cursor.fetchall()

    def write_data(self, table_name, data):
        """Schreibt neue Daten in eine PostgreSQL-Tabelle."""
        with self.connection.cursor() as cursor:
            # Annahme: data ist eine Liste von Tupeln
            placeholders = ', '.join(['%s'] * len(data[0]))
            query = f"INSERT INTO {table_name} VALUES ({placeholders})"
            cursor.executemany(query, data)
            self.connection.commit()

    def enrich_fact_table(self, fact_table, dim_table, fact_column, dim_column, key_column):
        """Reichert die Faktentabelle mit Primärschlüsseln aus einer Dimensionstabelle an."""

    def main(self):
        AWS_SPARK_ACCESS_KEY = os.getenv('MINIO_SPARK_ACCESS_KEY')
        AWS_SPARK_SECRET_KEY = os.getenv('MINIO_SPARK_SECRET_KEY')
        MINIO_IP_ADDRESS = socket.gethostbyname("minio")
        SPARK_MASTER_IP_ADDRESS = socket.gethostbyname("spark-master")

        s3_client_manager = JobS3ClientManager(
            AWS_SPARK_ACCESS_KEY, AWS_SPARK_SECRET_KEY, f"http://{MINIO_IP_ADDRESS}:9000")
        s3_client = s3_client_manager.get_boto_client()

        config_file = "config.yaml"
        config_part = "dimensions_info"
        config_tables = ConfigManager.load_config(config_file, config_part)

        # die neuen Daten (CSV) der Fact Tabelle laden aus S3

        # über jede Dim Tabelle aus dem Config iterieren und
        # die neuen Daten (CSV) aus S3 laden
        # diese Daten ins Postgres laden
        # die Primary Keys erhalten, alle Primary Keys
        # die Primary Keys in der Fact Tabelle einsetzen
