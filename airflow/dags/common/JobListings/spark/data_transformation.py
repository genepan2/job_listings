from constants import JOB_LEVELS
import helper_transform as HelperTransform

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import udf, col

import re
import hashlib


class DataTransformation:
    def __init__(self, args):
        self.spark_session_name = args[1]
        self.source_name = args[2]

    def transform_job_level(self, level, title):
        return HelperTransform.transform_job_level(level, title) if level else JOB_LEVELS["middle"]

    def clean_linkedin_id(self, linkedin_id):
        return linkedin_id.replace('<!--', '').replace('-->', '') if linkedin_id else None

    def clean_company_linkedin_url(self, company_url):
        return company_url.split('?')[0] if company_url else None

    def create_job_fingerprint(self, title, company, description):
        short_description = description[:256]

        combined_string = f"{title.lower()}|{company.lower()}|{short_description.lower()}"
        # remove all spcial characters
        clean_string = re.sub(r'\W+', '', combined_string)

        fingerprint = hashlib.sha256(clean_string.encode()).hexdigest()

        return fingerprint

    def transform_source_linkedin(self, df):

        df_filtered = df.dropna()
        # df_filtered.limit(20).show(truncate=False)

        # clean_string_udf = udf(HelperTransform.clean_string, StringType())
        transform_job_title_udf = udf(
            HelperTransform.transform_job_title, StringType())
        # transform_job_level_udf = udf(HelperTransform.transform_job_level, StringType())
        transform_job_location_udf = udf(
            HelperTransform.transform_job_location, StringType())
        transform_to_isoformat_udf = udf(
            HelperTransform.transform_to_isoformat, StringType())
        transform_detect_language_udf = udf(
            HelperTransform.transform_detect_language, StringType())
        # todo: this has to be modified, because there are certain wording which indicates vague applicants
        extract_applicants_udf = udf(lambda x: int(re.compile(
            r'\d+').findall(x)[0]) if x and re.compile(r'\d+').findall(x) else 0, IntegerType())
        transform_job_level_udf = udf(self.transform_job_level, StringType())
        clean_linkedin_id_udf = udf(self.clean_linkedin_id, StringType())
        clean_company_linkedin_url_udf = udf(
            self.clean_company_linkedin_url, StringType())
        create_job_fingerprint_udf = udf(
            self.create_job_fingerprint, StringType())

        df_cleaned = df_filtered \
            .withColumn("title_cleaned", transform_job_title_udf(col("title"))) \
            .withColumn("fingerprint", create_job_fingerprint_udf(col("title"), col("company_name"), col("description"))) \
            .withColumn("level", transform_job_level_udf(col("level"), col("title"))) \
            .withColumn("location", transform_job_location_udf(col("location"))) \
            .withColumn("publish_date", transform_to_isoformat_udf(col("publish_date"), col("search_datetime"))) \
            .withColumn("job_apps_count", extract_applicants_udf(col("job_apps_count"))) \
            .withColumn("language", transform_detect_language_udf(col("description"))) \
            .withColumn("source_identifier", clean_linkedin_id_udf(col("source_identifier"))) \
            .withColumn("company_linkedin_url", clean_company_linkedin_url_udf(col("company_linkedin_url"))) \
            .withColumnRenamed("Unnamed: 0", "index")  # important, otherwise error. spark needs all columns to be named

        return df_cleaned

    def transform(self, df):
        if self.source_name == 'linkedin':
            return self.transform_source_linkedin(df)
        # elif self.source_name == 'themuse':
        #     return self.transform_source_themuse(df)
        # elif self.source_name == 'whatjobs':
        #     return self.transform_source_whatjobs(df)
        else:
            raise ValueError(f"Unsupported data source: {self.source_name}")

    def get_df_schema_source_linkedin(self):
        schema = StructType([
            StructField("Unnamed: 0", IntegerType(), True),
            StructField("company_name", StringType(), True),
            StructField("company_linkedin_url", StringType(), True),
            StructField("title", StringType(), True),
            # StructField("title_cleaned", StringType(), True),
            StructField("location", StringType(), True),
            StructField("country", StringType(), True),
            StructField("source_identifier", StringType(), True),
            StructField("url", StringType(), True),
            StructField("publish_date", StringType(), True),
            StructField("level", StringType(), True),
            StructField("employment", StringType(), True),
            StructField("function", StringType(), True),
            StructField("industries", StringType(), True),
            StructField("description", StringType(), True),
            # StructField("search_datetime", TimestampType(), True),
            StructField("search_datetime", StringType(), True),
            StructField("search_keyword", StringType(), True),
            StructField("search_location", StringType(), True),
            StructField("job_apps_count", StringType(), True),
            StructField("scrape_dur_ms", StringType(), True),
            StructField("source", StringType(), True),
        ])
        return schema

    def get_df_schema(self, source_name):
        if source_name == 'linkedin':
            return self.get_df_schema_source_linkedin()
        # elif source_name == 'themuse':
        #     return self.get_df_schema_source_themuse()
        # elif source_name == 'whatjobs':
        #     return self.get_df_schema_source_whatjobs()
        else:
            raise ValueError(f"Unsupported data source: {source_name}")

    # def main(self):
    #     # get the parameters from SparkSubmitOperator
    #     spark = self.get_spark_session(self.spark_session_name)
    #     data_raw = self.load_recent_files(BUCKET_FROM, self.source_name)

    #     # convert pandas df to spark df
    #     schema = self.get_df_schema(self.source_name)
    #     spark_df = spark.createDataFrame(data_raw, schema=schema)

    #     data_clean = self.transform(spark_df, self.source_name)

    #     target_path_delta = f"s3a://{BUCKET_TO}/{self.source_name}_data"
    #     self.save_to_delta(data_clean, target_path_delta)

    #     # save as CSV for loading into PostgreSQL
    #     # timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    #     target_path_csv = f"s3a://{BUCKET_TO}/{self.source_name}_data_csv"
    #     self.save_as_csv(data_clean, target_path_csv)

    #     spark.stop()
