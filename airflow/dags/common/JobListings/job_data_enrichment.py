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

    def load_dimension_table(
        self, table_name, columns, dim_column=None, fact_value=None
    ):
        """
        Loads a dimension table and optionally filters based on a value.

        Parameters:
        table_name (str): The name of the dimension table.
        dim_column (str): Optional. The column name in the dimension table to filter on.
        fact_value (str): Optional. The value to filter in the dimension table.

        Returns:
        DataFrame: A DataFrame of the filtered (or unfiltered) dimension table.
        """

        columns_to_get = " ,".join(columns)

        # Load the entire dimension table
        dim_df = self.spark.read.jdbc(
            url=self.jdbc_url,
            # table=table_name,
            table=f"(SELECT {columns_to_get} FROM {table_name}) AS {table_name}_alias",
            properties=self.connection_prop,
        )

        # If a column name and value for filtering are provided, apply the filter
        if dim_column and fact_value:
            dim_df = dim_df.filter(dim_df[dim_column] == fact_value)

        return dim_df

    def save_dimension_table(self, df, table_name):
        df.write.format("jdbc").mode("append").option("url", self.jdbc_url).option(
            "dbtable", table_name
        ).save()

    def enrich_with_dimension(self, fact_df, dim_df, fact_col, dim_col, fk_col):
        # Beispiel: Anreicherung mit einer Dimensionstabelle
        return fact_df.join(
            dim_df, fact_df[fact_col] == dim_df[dim_col], "left"
        ).withColumn(fk_col, dim_df["id"])

    def run_enrichment_process(self, fact_df):
        # Beispiel: Ausführen des gesamten Anreicherungsprozesses
        dim_locations = self.load_dimension_table("dimLocations")
        enriched_df = self.enrich_with_dimension(
            fact_df, dim_locations, "location_name", "location_name", "location_fk"
        )
        # Weitere Anreicherungsschritte hier...
        return enriched_df
