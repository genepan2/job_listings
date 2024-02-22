import psycopg2
from psycopg2.extensions import register_adapter, AsIs

import os
import pandas as pd
import logging
import numpy as np

from job_s3_client_manager import JobS3ClientManager
from job_config_manager import JobConfigManager
from job_file_processing import JobFileProcessing

from job_helper_utils import (
    generate_id_column_name,
    generate_fact_key_column_name,
    generate_dim_table_name_from_id_column_name,
)

register_adapter(np.int64, AsIs)


class JobDataLoader:
    def __init__(self, source_name, bucket_name, tables_config):
        self.source_name = source_name
        self.config_manager = JobConfigManager(tables_config)
        self.connection = self.connect_to_db()
        self.bucket_name = bucket_name

    def connect_to_db(self):
        """Stellt eine Verbindung zur PostgreSQL-Datenbank her."""
        return psycopg2.connect(
            dbname=os.getenv("DW_DB"),
            user=os.getenv("DW_USER"),
            password=os.getenv("DW_PASS"),
            # host=os.getenv('postgres')
            host="postgres",
        )

    def dataframe_to_tuples(self, df):
        """Konvertiert einen Pandas DataFrame in eine Liste von Tupeln."""
        # # logging.info(df.info())
        # df.info()
        # for column in df.columns:
        #     # logging.info(f"Dtype is: {df[column].dtype}")
        #     if df[column].dtype == numpy.int64:
        #         df[column] = df[column].astype(int)
        # converted_df = df.applymap(lambda x: int(x) if isinstance(x, np.int64) else x)
        # if "date_unique" in df.columns:
        #     # logging.info("date_unique was found...")
        #     df["date_unique"] = df["date_unique"].values.astype(int)
        # # logging.info(converted_df.info())
        df.info()
        return [tuple(x) for x in df.to_numpy()]

    def write_dataframe(self, table_name, df, distinct_column=None, return_ids=False):
        """Schreibt Daten aus einem DataFrame in eine PostgreSQL-Tabelle und gibt die generierten IDs zurück."""
        # # logging.info(df.info())
        data_tuples = self.dataframe_to_tuples(df)  # Konvertiere den DataFrame in Tupel
        return self.write_data(
            table_name, data_tuples, df.columns, distinct_column, return_ids
        )

    def write_data(self, table_name, data, columns, distinct_column, return_ids):
        """Schreibt neue Daten in eine PostgreSQL-Tabelle."""
        # # logging.info(data.info())
        if not data:
            # logging.info("Keine Daten zum Einfügen.")
            # raise ValueError("Keine Daten zum Einfügen.")
            return

        # columns = data[0].keys()

        with self.connection.cursor() as cursor:
            try:
                column_names = ", ".join(columns)
                placeholders = ", ".join(["%s"] * len(columns))

                if distinct_column is None:
                    query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
                else:
                    query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders}) ON CONFLICT ({distinct_column}) DO NOTHING"

                if return_ids:
                    id_column_name = generate_id_column_name(table_name)
                    query += f" RETURNING {id_column_name}"

                for row in data:
                    cursor.execute(query, row)
                    if return_ids:
                        ids = [row[0] for row in cursor.fetchall()]
                        data_with_ids = [row + (id,) for row, id in zip(data, ids)]
                    else:
                        data_with_ids = data

                # Commit der Transaktion, wenn alle Einfügungen erfolgreich waren
                self.connection.commit()
                # logging.info(f"Table {table_name} successfuly written to data warehouse.")
            except Exception as e:
                self.connection.rollback()  # Rollback im Fehlerfall
                # Logge den Fehler oder sende eine Benachrichtigung
                logging.info(f"Fehler beim Einfügen in die Datenbank: {e}")
                # Optional: Hier könntest du den Fehler weiter nach außen werfen oder spezifische Aktionen durchführen
                raise

        return data_with_ids

    def get_dim_ids_for_fact_values(
        self, dim_table_name, fact_table_df, dim_value_column, fact_value_column
    ):
        """
        Holt die IDs aus der Dimensionstabelle für Werte, die in der Faktentabelle vorhanden sind.

        :param dim_table_name: Name der Dimensionstabelle in der Datenbank.
        :param fact_table_df: DataFrame der Faktentabelle.
        :param dim_value_column: Spaltenname in der Dimensionstabelle, der die Werte enthält.
        :param fact_value_column: Spaltenname in der Faktentabelle, der die Werte enthält.
        :return: DataFrame mit den Werten aus der Faktentabelle und den entsprechenden IDs aus der Dimensionstabelle.
        """
        # Erstellen einer Liste mit eindeutigen Werten aus der Faktentabelle
        unique_values = fact_table_df[fact_value_column].dropna().unique().tolist()
        # logging.info(unique_values)
        dim_id_column_name = generate_id_column_name(dim_table_name)
        # SQL-Abfrage vorbereiten, um IDs für diese Werte zu holen
        query = f"SELECT {dim_value_column}, {dim_id_column_name} FROM {dim_table_name} WHERE {dim_value_column} IN %s"
        # Ausführen der Abfrage und Speichern der Ergebnisse in einem DataFrame
        with self.connection.cursor() as cursor:
            cursor.execute(query, (tuple(unique_values),))
            result = cursor.fetchall()
            ids_df = pd.DataFrame(
                result, columns=[dim_value_column, dim_id_column_name]
            )

        return ids_df

    def enrich_fact_table_with_dim_ids(
        self,
        fact_table,
        dim_table_with_ids,
        fact_column_name,
        dim_column_name,
        fact_key_column_name,
        dim_id_column_name,
    ):
        """
        Reichert eine Faktentabelle mit IDs aus einer Dimensionstabelle an.

        :param fact_table: DataFrame der Faktentabelle.
        :param dim_table_with_ids: DataFrame der Dimensionstabelle, angereichert mit IDs.
        :param merge_column_name: Der Name der Spalte, über die die Faktentabelle und die Dimensionstabelle zusammengeführt werden.
        :param fact_key_column_name: Der Name der neuen Spalte in der Faktentabelle, die die IDs aus der Dimensionstabelle aufnehmen soll.
        :return: DataFrame der angereicherten Faktentabelle.
        """
        dim_table_name = generate_dim_table_name_from_id_column_name(dim_id_column_name)
        logging.info(f"Dim  Table Name: {dim_table_name}")
        logging.info(f"Fact Column Name: {fact_column_name}")
        logging.info(f"Dim  Column Name: {dim_column_name}")

        dim_table_with_ids.drop_duplicates(subset=[dim_column_name], inplace=True)
        logging.info(dim_table_with_ids.head(10))
        # Zusammenführen der Faktentabelle mit der Dimensionstabelle, um die IDs hinzuzufügen
        enriched_fact_table = fact_table.merge(
            dim_table_with_ids,
            left_on=fact_column_name,
            right_on=dim_column_name,
            how="left",
            suffixes=("", f"_{dim_table_name}"),
        )

        # Umbenennen der ID-Spalte in der angereicherten Faktentabelle, falls notwendig
        enriched_fact_table.rename(
            columns={dim_id_column_name: fact_key_column_name}, inplace=True
        )
        # logging.info(enriched_fact_table[fact_key_column_name])
        # logging.info(f"Count Rows: {enriched_fact_table.shape[0]}")
        return enriched_fact_table

    def process_fact_table(
        self,
        dim_table_name,
        fact_data_df,
        dim_column_name,
        fact_column_names,
        fact_key_column_names,
    ):
        # Stelle sicher, dass fact_column_names und fact_key_column_names Listen sind
        if not isinstance(fact_column_names, list):
            fact_column_names = [fact_column_names]
        if not isinstance(fact_key_column_names, list):
            fact_key_column_names = [fact_key_column_names]

        dim_id_column_name = generate_id_column_name(dim_table_name)

        # Iteriere über jede Spalte, die verarbeitet werden soll
        for fact_column_name, fact_key_column_name in zip(
            fact_column_names, fact_key_column_names
        ):
            new_ids_df = self.get_dim_ids_for_fact_values(
                dim_table_name, fact_data_df, dim_column_name, fact_column_name
            )

            # die Primary Keys in der Fact Tabelle einsetzen
            fact_data_df = self.enrich_fact_table_with_dim_ids(
                fact_data_df,
                new_ids_df,
                fact_column_name,
                dim_column_name,
                fact_key_column_name,
                dim_id_column_name,
            )

        return fact_data_df

    def main(self):
        # config_manager = JobConfigManager("job_config_tables.yaml")
        dim_tables_config = self.config_manager.load_config("dimensions_info")
        fact_table_name = self.config_manager.get_fact_table_name()

        s3_manager = JobS3ClientManager()
        # s3_client = s3_manager.get_boto_client()

        file_processor = JobFileProcessing(
            s3_manager, self.bucket_name, self.source_name
        )

        # die neuen Daten (CSV) der Fact Tabelle laden aus S3
        fact_data_df = file_processor.merge_files_to_df(fact_table_name)
        fact_data_df.info()

        # über jede Dim Tabelle aus dem Config iterieren und
        for dim_table_name, dim_table_info in dim_tables_config.items():
            # die neuen Daten der Dim Tabelle (CSV) aus S3 laden
            dim_data_df = file_processor.merge_files_to_df(dim_table_name)
            # dim_data_df = (
            #     dim_data_df.dropna().drop_duplicates(inplace=True)
            #     # .reset_index(drop=True)
            # )

            # i want to remove duplicates from the dim data based on one column
            uniqueColumn = dim_table_info["uniqueColumns"][0]
            dim_data_df = dim_data_df.drop_duplicates(subset=[uniqueColumn])

            # if dim_data_df.empty:
            # continue
            # diese Daten ins Postgres laden
            # die Primary Keys erhalten, alle Primary Keys
            # dim_id_column_name = generate_id_column_name(dim_table_name)

            if not dim_table_info.get("factForeignKeyColumns"):
                dim_table_info["factForeignKeyColumns"] = generate_fact_key_column_name(
                    dim_table_name
                )
            fact_key_column_names = dim_table_info["factForeignKeyColumns"]

            fact_column_names = dim_table_info["factColumns"]
            dim_column_name = dim_table_info["dimColumns"]

            if not dim_data_df.empty:
                # continue
                self.write_dataframe(dim_table_name, dim_data_df, uniqueColumn)

            # new_ids_df = pd.DataFrame(new_ids, columns=[dim_id_column_name])
            logging.info(
                f"Count Rows in Dim  Table {dim_table_name}: {dim_data_df.shape[0]}"
            )

            fact_data_df.info()
            logging.info(
                f"Count Rows in Fact Table                 : {fact_data_df.shape[0]}"
            )
            fact_data_df = self.process_fact_table(
                dim_table_name,
                fact_data_df,
                dim_column_name,
                fact_column_names,
                fact_key_column_names,
            )
            logging.info(f"Last Dim Table: {dim_table_name}")
            fact_data_df.info()
            logging.info(
                f"Count Rows in Fact Table                 : {fact_data_df.shape[0]}"
            )
            fact_data_df.drop_duplicates(inplace=True)

        # jetzt muss die fact tabelle in die datenbank geladen werden.
        # und zwar nur die notwendigen spalten

        fact_table_columns = self.config_manager.get_all_fact_table_columns()
        fact_data_df.info()
        final_fact_table_df = fact_data_df[fact_table_columns]

        # logging.info(final_fact_table_df.head(25))
        # logging.info(f"Count Rows: {final_fact_table_df.shape[0]}")

        self.write_dataframe(fact_table_name, final_fact_table_df)
