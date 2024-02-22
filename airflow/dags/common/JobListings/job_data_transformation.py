from job_config_constants import JOB_LEVELS
import job_helper_transform as JobHelperTransform

import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    TimestampType,
)
from pyspark.sql.functions import udf, col, lit, to_timestamp, format_string
from pyspark.sql.functions import (
    year,
    month,
    weekofyear,
    dayofmonth,
    hour,
    minute,
    dayofweek,
)

import re
import hashlib
import logging
from pyspark.sql.functions import isnan


class JobDataTransformation:
    def __init__(self, args):
        self.spark_session_name = args[1]
        self.source_name = args[2]

        # spark_session.sparkContext.setLogLevel("ERROR")
        # log4jLogger = spark_session._jvm.org.apache.log4j
        # logger = log4jLogger.LogManager.getLogger("TRANSFORMER")
        # logger.setLevel(log4jLogger.Level.INFO)
        # self.logger = logger

    def transform_job_level(self, level, title):
        return (
            JobHelperTransform.transform_job_level(level, title)
            if level
            else JOB_LEVELS["middle"]
        )

    def clean_linkedin_id(self, linkedin_id):
        return (
            linkedin_id.replace("<!--", "").replace("-->", "") if linkedin_id else None
        )

    def clean_company_linkedin_url(self, company_url):
        return company_url.split("?")[0] if company_url else None

    def create_job_fingerprint(self, title, company, description):
        short_description = description[:256]

        combined_string = (
            f"{title.lower()}|{company.lower()}|{short_description.lower()}"
        )
        # remove all spcial characters
        clean_string = re.sub(r"\W+", "", combined_string)

        fingerprint = hashlib.sha256(clean_string.encode()).hexdigest()

        return fingerprint

    def extend_df_with_date_info(self, df, date_column_name):
        # Fügt dem DataFrame zusätzliche Datumsspalten hinzu
        df_extended = (
            df.withColumn(
                f"{date_column_name}_unique",
                format_string(
                    "%04d%02d%02d%02d%02d",
                    year(col(date_column_name)),
                    month(col(date_column_name)),
                    dayofmonth(col(date_column_name)),
                    hour(col(date_column_name)),
                    minute(col(date_column_name)),
                ),
            )
            .withColumn(f"{date_column_name}_year", year(col(date_column_name)))
            .withColumn(f"{date_column_name}_month", month(col(date_column_name)))
            .withColumn(f"{date_column_name}_week", weekofyear(col(date_column_name)))
            .withColumn(f"{date_column_name}_day", dayofmonth(col(date_column_name)))
            .withColumn(f"{date_column_name}_hour", hour(col(date_column_name)))
            .withColumn(f"{date_column_name}_minute", minute(col(date_column_name)))
            .withColumn(
                f"{date_column_name}_week_day", dayofweek(col(date_column_name))
            )
            .withColumn(f"{date_column_name}_is_holiday", lit(False))
        )
        return df_extended

    def apply_multiple_date_transformations(self, df, columns):
        df = self.extend_df_with_date_info(df, columns[0])
        df = self.extend_df_with_date_info(df, columns[1])
        return df

    def transform_source_linkedin(self, df):
        # df_filtered = df.dropna()
        df.limit(20).show(truncate=False)

        # clean_string_udf = udf(JobHelperTransform.clean_string, StringType())
        transform_job_title_udf = udf(
            JobHelperTransform.transform_job_title, StringType()
        )
        # transform_job_level_udf = udf(JobHelperTransform.transform_job_level, StringType())
        transform_job_location_udf = udf(
            JobHelperTransform.transform_job_location, StringType()
        )
        transform_to_isoformat_udf = udf(
            JobHelperTransform.transform_to_isoformat, StringType()
        )
        transform_detect_language_udf = udf(
            JobHelperTransform.transform_detect_language, StringType()
        )
        # todo: this has to be modified, because there are certain wording which indicates vague applicants
        extract_applicants_udf = udf(
            lambda x: int(re.compile(r"\d+").findall(x)[0])
            if x and re.compile(r"\d+").findall(x)
            else 0,
            IntegerType(),
        )
        transform_job_level_udf = udf(self.transform_job_level, StringType())
        clean_linkedin_id_udf = udf(self.clean_linkedin_id, StringType())
        clean_company_linkedin_url_udf = udf(
            self.clean_company_linkedin_url, StringType()
        )
        create_job_fingerprint_udf = udf(self.create_job_fingerprint, StringType())

        df_cleaned = (
            df.withColumn("title_cleaned", transform_job_title_udf(col("title")))
            .withColumn(
                "fingerprint",
                create_job_fingerprint_udf(
                    col("title"), col("company_name"), col("description")
                ),
            )
            .withColumn("level", transform_job_level_udf(col("level"), col("title")))
            .withColumn("location", transform_job_location_udf(col("location")))
            .withColumn(
                "publish_date",
                transform_to_isoformat_udf(col("publish_date"), col("search_datetime")),
            )
            .withColumn("job_apps_count", extract_applicants_udf(col("job_apps_count")))
            .withColumn("language", transform_detect_language_udf(col("description")))
            .withColumn(
                "source_identifier", clean_linkedin_id_udf(col("source_identifier"))
            )
            .withColumn(
                "company_linkedin_url",
                clean_company_linkedin_url_udf(col("company_linkedin_url")),
            )
            .withColumnRenamed("Unnamed: 0", "index")
        )  # important, otherwise error. spark needs all columns to be named

        df_cleaned = self.apply_multiple_date_transformations(
            df_cleaned, ["publish_date", "search_datetime"]
        )

        return df_cleaned

    # i need a method which removes all rows with NaN values in a spark dataframe
    def remove_nan_rows(self, df):
        rows_count = df.count()
        # df.printSchema()
        df_with_nulls = df.na.replace("NaN", None)
        df_cleaned = df_with_nulls.dropna(how="any")
        rows_cleaned_count = df_cleaned.count()
        # df_cleaned.printSchema()
        print(f"Number of rows at beginning: {rows_count}")
        print(f"Number of rows with NaN values: {rows_count - rows_cleaned_count}")
        return df_cleaned

    # def remove_nan_rows(self, df):
    #     nan_count = df.filter(df.isnull().any()).count()
    #     logging.info(f"Number of rows with NaN values: {nan_count}")
    #     return df.dropna()

    def transform(self, df):
        df_cleaned = self.remove_nan_rows(df)
        if self.source_name == "linkedin":
            return self.transform_source_linkedin(df_cleaned)
            # return self.transform_source_linkedin(df)
        # elif self.source_name == 'themuse':
        #     return self.transform_source_themuse(df)
        # elif self.source_name == 'whatjobs':
        #     return self.transform_source_whatjobs(df)
        else:
            raise ValueError(f"Unsupported data source: {self.source_name}")

    def get_df_schema_source_linkedin(self):
        schema = StructType(
            [
                StructField("Unnamed: 0", StringType(), True),
                StructField("company_name", StringType(), True),
                StructField("company_linkedin_url", StringType(), True),
                StructField("title", StringType(), True),
                StructField("location", StringType(), True),
                StructField("country", StringType(), True),
                StructField("source_identifier", StringType(), True),
                StructField("url", StringType(), True),
                StructField("job_apps_count", StringType(), True),
                StructField("publish_date", StringType(), True),
                StructField("level", StringType(), True),
                StructField("employment", StringType(), True),
                StructField("function", StringType(), True),
                StructField("industries", StringType(), True),
                StructField("description", StringType(), True),
                StructField("search_datetime", StringType(), True),
                StructField("search_keyword", StringType(), True),
                StructField("search_location", StringType(), True),
                StructField("source", StringType(), True),
                StructField("scrape_dur_ms", StringType(), True),
                StructField("Scraping_Pause", StringType(), True),
                # StructField("scrape_dur_ms", FloatType(), True),
            ]
        )
        return schema

    def get_df_schema(self, source_name):
        if source_name == "linkedin":
            return self.get_df_schema_source_linkedin()
        # elif source_name == 'themuse':
        #     return self.get_df_schema_source_themuse()
        # elif source_name == 'whatjobs':
        #     return self.get_df_schema_source_whatjobs()
        else:
            raise ValueError(f"Unsupported data source: {source_name}")

    def get_dataframes_from_data(self, df):
        return {
            "dim_jobs_df": df.select(
                "title",
                "title_cleaned",
                "description",
                "fingerprint",
            ),
            "dim_source_infos_df": df.select(
                "url",
                col("source_identifier").alias("identifier"),
            ),
            "dim_locations_df": df.select(
                col("location").alias("city"), "country"
            ),  # country is not there yet!
            "dim_languages_df": df.select(col("language").alias("name")),
            "dim_sources_df": df.select(
                col("source").alias("name")
            ),  # source is not there
            "dim_job_levels_df": df.select(col("level").alias("name")),
            "dim_search_keywords_df": df.select(col("search_keyword").alias("name")),
            "dim_search_locations_df": df.select(col("search_location").alias("name")),
            # "dim_dates_df": df.select(),
            # "dim_dates_df": date_df,
            "dim_employments_df": df.select(col("employment").alias("name")),
            "dim_industries_df": df.select(col("industries").alias("name")),
            # "dim_skill_categories_df": df.select(),
            # "dim_technology_categories_df": df.select(),
            # "dim_skills_df": df.select(),
            # "dim_technologies_df": df.select(),
            "dim_companies_df": df.select(col("company_name").alias("name")),
        }

    def select_fact_columns(self, df):
        return df.select(
            "company_name",
            "title",
            "location",
            "job_apps_count",
            "level",
            "employment",
            "industries",
            "search_datetime",
            "search_keyword",
            "search_location",
            "fingerprint",
            "language",
            "scrape_dur_ms",
            "source",
            "publish_date_unique",
            "publish_date_year",
            "publish_date_month",
            "publish_date_week",
            "publish_date_day",
            "publish_date_hour",
            "publish_date_minute",
            "publish_date_week_day",
            "publish_date_is_holiday",
            "search_datetime_unique",
            "search_datetime_year",
            "search_datetime_month",
            "search_datetime_week",
            "search_datetime_day",
            "search_datetime_hour",
            "search_datetime_minute",
            "search_datetime_week_day",
            "search_datetime_is_holiday",
        )

    def get_unique_date_values_dataframes(self, df):
        # Definiere die Spaltennamen für date_df1 und date_df2
        columns_df1 = [
            "publish_date_unique",
            "publish_date_year",
            "publish_date_month",
            "publish_date_week",
            "publish_date_day",
            "publish_date_hour",
            "publish_date_minute",
            "publish_date_week_day",
            "publish_date_is_holiday",
        ]
        columns_df2 = [
            "search_datetime_unique",
            "search_datetime_year",
            "search_datetime_month",
            "search_datetime_week",
            "search_datetime_day",
            "search_datetime_hour",
            "search_datetime_minute",
            "search_datetime_week_day",
            "search_datetime_is_holiday",
        ]

        date_df1 = self.create_df_with_aliases(df, columns_df1, "publish_date_")
        date_df2 = self.create_df_with_aliases(df, columns_df2, "search_datetime_")

        date_df = date_df1.union(date_df2)
        date_df_distinct = date_df.dropDuplicates(["unique"])
        date_df_renamed = date_df_distinct.withColumnRenamed("unique", "date_unique")

        return date_df_renamed

    def create_df_with_aliases(self, data_df, columns, remove_prefix):
        # Erstelle eine Liste von select-Anweisungen, die die ursprünglichen Spaltennamen in die gewünschten Aliase umwandelt
        select_expr = [
            col(column).alias(column.replace(remove_prefix, ""))
            if column.startswith(remove_prefix)
            else col(column)
            for column in columns
        ]
        # Erstelle den neuen DataFrame basierend auf den ausgewählten und umbenannten Spalten
        return data_df.select(*select_expr)
