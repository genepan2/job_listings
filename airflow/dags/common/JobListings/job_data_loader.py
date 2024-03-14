import psycopg2
from psycopg2.extensions import register_adapter, AsIs
from psycopg2.extras import execute_values
from psycopg2 import pool

import os
import pandas as pd
import logging
import numpy as np

from job_s3_client_manager import JobS3ClientManager
from job_config_manager import JobConfigManager
from job_file_processing import JobFileProcessing
from job_db_connection_pool import JobDBConnectionPool

from job_helper_utils import (
    generate_id_column_name,
    generate_fct_key_column_name,
    generate_dim_table_name_from_id_column_name,
    generate_bridge_key_column_name,
)

register_adapter(np.int64, AsIs)


class JobDataLoader:
    def __init__(self, source_name, bucket_name, tables_config):
        self.source_name = source_name
        self.config_manager = JobConfigManager(tables_config)
        # self.connection = self.connect_to_db()
        self.bucket_name = bucket_name
        self.connection_pool = JobDBConnectionPool(1, 5)

    # def connect_to_db(self):
    #     """Stellt eine Verbindung zur PostgreSQL-Datenbank her."""
    #     return psycopg2.connect(
    #         dbname=os.getenv("DW_DB"),
    #         user=os.getenv("DW_USER"),
    #         password=os.getenv("DW_PASS"),
    #         host="postgres",
    #     )

    # def init_connection_pool(self, minconn, maxconn):
    #     return pool.SimpleConnectionPool(
    #         minconn,
    #         maxconn,
    #         user=os.getenv("DW_USER"),
    #         password=os.getenv("DW_PASS"),
    #         host="postgres",
    #         port="5432",
    #         database=os.getenv("DW_DB"),
    #     )

    # def get_connection(self):
    #     return self.connection_pool.getconn()

    # def release_connection(self, conn):
    #     return self.connection_pool.putconn(conn)

    # def close_connection_pool(self):
    #     self.connection_pool.closeall()

    def dataframe_to_tuples(self, df):
        """Konvertiert einen Pandas DataFrame in eine Liste von Tupeln."""
        return [tuple(x) for x in df.to_numpy()]

    def write_dataframe(self, table_name, df, distinct_column=None, return_ids=False):
        """Schreibt Daten aus einem DataFrame in eine PostgreSQL-Tabelle und gibt die generierten IDs zurück."""
        data_tuples = self.dataframe_to_tuples(df)  # Konvertiere den DataFrame in Tupel
        return self.write_data(
            table_name, data_tuples, df.columns, distinct_column, return_ids
        )

    def write_data(self, table_name, data, columns, distinct_column, return_ids):
        """Schreibt neue Daten in eine PostgreSQL-Tabelle."""
        if not data:
            logging.warning("Keine Daten zum Einfügen.")
            # raise ValueError("Keine Daten zum Einfügen.")
            return

        # columns = data[0].keys()
        df = pd.DataFrame(data, columns=columns)

        conn = self.connection_pool.get_connection()

        with conn.cursor() as cursor:
            try:
                column_names = ", ".join(columns)
                placeholders = ", ".join(["%s"] * len(columns))

                query = self.construct_query(
                    table_name, column_names, placeholders, distinct_column, return_ids
                )

                if return_ids:
                    id_column_name = generate_id_column_name(table_name)

                    data_with_ids = []
                    for row in data:
                        logging.info(row)
                        logging.info(query)
                        cursor.execute(query, row)
                        id_of_new_row = (
                            cursor.fetchone()[0] if cursor.rowcount > 0 else None
                        )

                        # Kombiniere das Originaldaten-Tupel mit der neuen ID
                        new_row_with_id = row + (id_of_new_row,)
                        logging.info(new_row_with_id)
                        data_with_ids.append(new_row_with_id)

                    # self.connection.commit()
                    conn.commit()
                    column_names_with_id = columns.tolist() + [id_column_name]
                    df = pd.DataFrame(data_with_ids, columns=column_names_with_id)

                else:
                    execute_values(cursor, query, data)  # Bulk-Insert as of version 2.7
                    # self.connection.commit()
                    conn.commit()

            except Exception as e:
                # self.connection.rollback()
                conn.rollback()
                logging.error(f"Fehler beim Einfügen in die Datenbank: {e}")
                # logging.error(f"Query: {query}")
                # logging.error(f"Row: {row}")
                raise

        # self.release_connection(conn)
        self.connection_pool.release_connection(conn)
        return df

    def construct_query(
        self, table_name, column_names, placeholders, distinct_column, return_ids
    ):
        query = f"INSERT INTO {table_name} ({column_names})"

        if return_ids:
            query += f" VALUES ({placeholders})"
        else:
            query += " VALUES %s"

        if distinct_column:
            query += f" ON CONFLICT ({distinct_column}) DO NOTHING"

        if return_ids:
            id_column_name = generate_id_column_name(table_name)
            query += f" RETURNING {id_column_name}"

        return query

    def get_dim_ids_for_fct_values(
        self, dim_table_name, fct_table_df, dim_value_column, fct_value_column
    ):
        """
        Holt die IDs aus der Dimensionstabelle für Werte, die in der Faktentabelle vorhanden sind.

        :param dim_table_name: Name der Dimensionstabelle in der Datenbank.
        :param fct_table_df: DataFrame der Faktentabelle.
        :param dim_value_column: Spaltenname in der Dimensionstabelle, der die Werte enthält.
        :param fct_value_column: Spaltenname in der Faktentabelle, der die Werte enthält.
        :return: DataFrame mit den Werten aus der Faktentabelle und den entsprechenden IDs aus der Dimensionstabelle.
        """
        # Erstellen einer Liste mit eindeutigen Werten aus der Faktentabelle
        unique_values = fct_table_df[fct_value_column].dropna().unique().tolist()
        dim_id_column_name = generate_id_column_name(dim_table_name)

        # SQL-Abfrage vorbereiten, um IDs für diese Werte zu holen
        query = f"SELECT {dim_value_column}, {dim_id_column_name} FROM {dim_table_name} WHERE {dim_value_column} IN %s"

        conn = self.connection_pool.get_connection()

        # Ausführen der Abfrage und Speichern der Ergebnisse in einem DataFrame
        with conn.cursor() as cursor:
            cursor.execute(query, (tuple(unique_values),))
            result = cursor.fetchall()
            ids_df = pd.DataFrame(
                result, columns=[dim_value_column, dim_id_column_name]
            )

        # self.release_connection(conn)
        self.connection_pool.release_connection(conn)
        return ids_df

    def enrich_fct_table_with_dim_ids(
        self,
        fct_table_df,
        dim_table_with_ids,
        merge_on_fct_column_name,
        merge_on_dim_column_name,
        fct_key_column_name,
        dim_id_column_name,
    ):
        """
        Reichert eine Faktentabelle mit IDs aus einer Dimensionstabelle an.

        :param fct_table_df: DataFrame der Faktentabelle.
        :param dim_table_with_ids: DataFrame der Dimensionstabelle, angereichert mit IDs.
        :param merge_column_name: Der Name der Spalte, über die die Faktentabelle und die Dimensionstabelle zusammengeführt werden.
        :param fct_key_column_name: Der Name der neuen Spalte in der Faktentabelle, die die IDs aus der Dimensionstabelle aufnehmen soll.
        :return: DataFrame der angereicherten Faktentabelle.
        """
        dim_table_name = generate_dim_table_name_from_id_column_name(dim_id_column_name)
        # logging.info(f"Dim  Table Name: {dim_table_name}")
        # logging.info(f"Fact Column Name: {fct_column_name}")
        # logging.info(f"Dim  Column Name: {dim_column_name}")

        dim_table_with_ids.drop_duplicates(
            subset=[merge_on_dim_column_name], inplace=True
        )
        # logging.info(dim_table_with_ids.head(10))
        # Zusammenführen der Faktentabelle mit der Dimensionstabelle, um die IDs hinzuzufügen
        enriched_fct_table = fct_table_df.merge(
            dim_table_with_ids,
            left_on=merge_on_fct_column_name,
            right_on=merge_on_dim_column_name,
            how="left",
            suffixes=("", f"_{dim_table_name}"),
        )

        # Umbenennen der ID-Spalte in der angereicherten Faktentabelle, falls notwendig
        enriched_fct_table.rename(
            columns={dim_id_column_name: fct_key_column_name}, inplace=True
        )
        # logging.info(enriched_fct_table[fct_key_column_name])
        # logging.info(f"Count Rows: {enriched_fct_table.shape[0]}")
        return enriched_fct_table

    def process_fct_table(
        self,
        dim_table_name,
        fct_data_df,
        dim_column_name,
        fct_column_names,
        fct_key_column_names,
    ):
        # Stelle sicher, dass fct_column_names und fct_key_column_names Listen sind
        if not isinstance(fct_column_names, list):
            fct_column_names = [fct_column_names]
        if not isinstance(fct_key_column_names, list):
            fct_key_column_names = [fct_key_column_names]

        dim_id_column_name = generate_id_column_name(dim_table_name)

        # Iteriere über jede Spalte, die verarbeitet werden soll
        for fct_column_name, fct_key_column_name in zip(
            fct_column_names, fct_key_column_names
        ):
            dim_new_ids_df = self.get_dim_ids_for_fct_values(
                dim_table_name, fct_data_df, dim_column_name, fct_column_name
            )

            # die Primary Keys in der Fact Tabelle einsetzen
            fct_data_df = self.enrich_fct_table_with_dim_ids(
                fct_data_df,
                dim_new_ids_df,
                fct_column_name,
                dim_column_name,
                fct_key_column_name,
                dim_id_column_name,
            )

        return fct_data_df

    def main(self):
        s3_manager = JobS3ClientManager()
        file_processor = JobFileProcessing(
            s3_manager, self.bucket_name, self.source_name
        )

        # config_manager = JobConfigManager("job_config_tables.yaml")
        fct_table_name = self.config_manager.get_fct_table_name()
        fct_uniqueColumns = self.config_manager.get_fct_unique_columns()
        fct_uniqueColumn = fct_uniqueColumns[0]

        # die neuen Daten (CSV) der Fact Tabelle laden aus S3
        fct_data_df = file_processor.merge_files_to_df(fct_table_name)
        fct_data_df.drop_duplicates(subset=fct_uniqueColumn, inplace=True)

        #
        # starting working on dim tables
        #

        # iterate over all dim tables
        dim_tables_config = self.config_manager.load_config("dimensions_info")
        for dim_table_name, dim_table_info in dim_tables_config.items():
            logging.info(f"Doing Dim Table: {dim_table_name}")

            # load new dim table data from S3 bucket
            dim_data_df = file_processor.merge_files_to_df(dim_table_name)

            # if dim_data_df.empty:
                # continue

            uniqueColumn = dim_table_info["uniqueColumns"][0]
            dim_data_df.drop_duplicates(subset=[uniqueColumn], inplace=True)
            # .reset_index(drop=True)

            # is_bridge = dim_table_info.get("isBridge")

            if not dim_table_info.get("factForeignKeyColumns"):
                dim_table_info["factForeignKeyColumns"] = generate_fct_key_column_name(
                    dim_table_name
                )
            fct_key_column_names = dim_table_info["factForeignKeyColumns"]

            fct_column_names = dim_table_info["factColumns"]
            dim_column_name = dim_table_info["dimColumns"]

            if not dim_data_df.empty:
                # here we write only new values to the dim table, which were not present yet. no need to return ids.
                self.write_dataframe(dim_table_name, dim_data_df, uniqueColumn)
                logging.info(f"Saved Dim Table: {dim_table_name}")

            # here were set the foreign keys of the dim table into the fact table
            fct_data_df = self.process_fct_table(
                dim_table_name,
                fct_data_df,
                dim_column_name,
                fct_column_names,
                fct_key_column_names,
            )
            logging.info(f"Updated Fact Table with Dim Table: {dim_table_name}")

            fct_data_df.drop_duplicates(inplace=True)

        #
        # starting working on fact table
        #

        logging.info(f"Doing Fact Table: {fct_table_name}")
        fct_table_columns = self.config_manager.get_all_fct_table_columns()
        bridge_column_names = self.config_manager.get_fct_table_bridge_columns()

        # maybe to complicated...
        final_fct_table_columns = [
            col for col in fct_table_columns if col not in bridge_column_names
        ]
        print(final_fct_table_columns)
        
        final_fct_table_df = fct_data_df[final_fct_table_columns]

        # write the fact table to the database and return the ids
        fct_table_df_with_ids = self.write_dataframe(
            fct_table_name, final_fct_table_df, fct_uniqueColumn, True
        )
        logging.info(f"Saved Fact Table: {fct_table_name}")

        #
        # starting working on bridge tables
        #

        final_fct_table_bridge_columns_df = fct_data_df[
            bridge_column_names + [fct_uniqueColumn]
        ]

        final_fct_table_bridge_columns_df.loc[
            :, fct_uniqueColumn
        ] = final_fct_table_bridge_columns_df[fct_uniqueColumn].astype(str)

        # merging the fact table with the bridge columns, so the fact table is complete again
        complete_fct_df = pd.merge(
            fct_table_df_with_ids,
            final_fct_table_bridge_columns_df,
            on=fct_uniqueColumn,
            how="inner",
        )

        # now we are processing the bringe tables
        bridge_tables_config = self.config_manager.get_bridge_tables()
        fct_id_column_name = generate_id_column_name(fct_table_name)
        fct_key_column_name = fct_id_column_name[:-3] + "_key"

        # iterate over all bridge tables
        for bridge_table_name, bridge_table_info in bridge_tables_config.items():
            logging.info(f"Doing Bridge Table: {bridge_table_name}")
            dim_key_column_name = generate_bridge_key_column_name(bridge_table_name)

            if dim_key_column_name not in complete_fct_df.columns:
                logging.info(
                    f"{dim_key_column_name} Column not found in Fact Table. Skipping Bridge Table: {bridge_table_name}"
                )
                continue

            bridge_df = complete_fct_df[[fct_id_column_name, dim_key_column_name]]
            bridge_df.rename(
                inplace=True, columns={fct_id_column_name: fct_key_column_name}
            )

            self.write_dataframe(bridge_table_name, bridge_df)
            logging.info(f"Saved Bridge Table: {bridge_table_name}")

        # self.close_connection_pool()
        self.connection_pool.close_all_connections()
        logging.info("Finished Job Data Loader")
