import os


class JobDataEnrichment:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.connection_prop = {
            "user": os.getenv("DW_USER"),
            "password": os.getenv("DW_PASS"),
            "driver": "org.postgresql.Driver",
        }
        self.jdbc_url = os.getenv("JDBC_URL")

    def load_filtered_table(
        self, table_name, columns, match_column, match_df, match_column_df
    ):
        """
        Loads table rows that match values in a given DataFrame column.

        Parameters:
        table_name (str): The name of the table.
        columns (list[str]): The columns to load from the table.
        match_column (str): The column name in the table to match against.
        match_df (DataFrame): The DataFrame containing values to match.
        match_column_df (str): The column name in match_df containing the match values.

        Returns:
        DataFrame: A DataFrame of the filtered table.
        """

        # Convert the DataFrame column to a list of unique values
        match_values = list(
            match_df.select(match_column_df).distinct().toPandas()[match_column_df]
        )

        # Format the match values for SQL query
        match_values_str = ",".join([f"'{value}'" for value in match_values])

        # Build the SQL query
        columns_to_get = ", ".join(columns)
        query = f"""
            (SELECT {columns_to_get} FROM {table_name}
            WHERE {match_column} IN ({match_values_str})) AS {table_name}_alias
        """

        # Load the filtered table
        filtered_df = self.spark.read.jdbc(
            url=self.jdbc_url,
            table=query,
            properties=self.connection_prop,
        )

        # print("type of filtered_df:")
        # print(type(filtered_df))
        # <class 'pyspark.sql.dataframe.DataFrame'>
        return filtered_df

    def save_dimension_table(self, df, table_name):
        df.write.format("jdbc").mode("append").option("url", self.jdbc_url).option(
            "dbtable", table_name
        ).save()

    def enrich_with_dimension(self, fct_df, dim_df, fct_col, dim_col, fk_col):
        # Beispiel: Anreicherung mit einer Dimensionstabelle
        return fct_df.join(
            dim_df, fct_df[fct_col] == dim_df[dim_col], "left"
        ).withColumn(fk_col, dim_df["id"])

    def run_enrichment_process(self, fct_df):
        # Beispiel: Ausführen des gesamten Anreicherungsprozesses
        dim_locations = self.load_dimension_table("dimLocations")
        enriched_df = self.enrich_with_dimension(
            fct_df, dim_locations, "location_name", "location_name", "location_fk"
        )
        # Weitere Anreicherungsschritte hier...
        return enriched_df
