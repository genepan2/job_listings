import os
from datetime import datetime
import re
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common.JobListings.constants import PATH, COLLECTIONS


def get_collection_keys_without_all():
    filtered_keys = [key for key in COLLECTIONS.keys() if key != "all"]
    return filtered_keys


def construct_file_path_for_data_source(source_name):
    return os.path.join(PATH["data_processed"], source_name + '_json', source_name + '_cleaned_data.json')


def create_key_name(source_name, is_raw=True, search_location=None, search_keyword=None):
    now = sanitize_filename(datetime.now().isoformat(), True)
    location = sanitize_filename(search_location) if search_location else ""
    keyword = sanitize_filename(search_keyword) if search_keyword else ""
    source = sanitize_filename(source_name)

    data_state = "raw" if is_raw else "cleaned"

    parts = [source, data_state, now, location, keyword]
    return "_".join(part for part in parts if part)


def sanitize_filename(string, replace=False):
    # note the backslash in front of the "-". otherwise it means from to.
    pattern = "[,!.\-: ]"
    # logging.info(string)
    if replace == False:
        filename = re.sub(pattern, "_", string)
    else:
        filename = re.sub(pattern, "", string)

    return filename.lower().replace("__", "_")


def get_spark_session(appname, endpoint_url,
                      access_key, secret_key):

    spark = (SparkSession.builder
             .appName(appname)
             .config("spark.network.timeout", "10000s")
             #  .config("hive.metastore.uris", hive_metastore)
             #  .config("hive.exec.dynamic.partition", "true")
             #  .config("hive.exec.dynamic.partition.mode", "nonstrict")
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
             .config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "true")
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
             .config("spark.hadoop.fs.s3a.fast.upload", "true")
             .config("spark.hadoop.fs.s3a.endpoint", endpoint_url)
             .config("spark.hadoop.fs.s3a.access.key", access_key)
             .config("spark.hadoop.fs.s3a.secret.key", secret_key)
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.history.fs.logDirectory", "s3a://spark/")
             .config("spark.sql.files.ignoreMissingFiles", "true")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
             #  .enableHiveSupport()
             .getOrCreate())
    return spark
