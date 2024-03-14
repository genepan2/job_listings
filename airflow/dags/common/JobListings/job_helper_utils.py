import os
from datetime import datetime
import re
# from pyspark.sql import SparkSession

# from pyspark.sql import functions as F
from stringcase import snakecase, camelcase, pascalcase

from job_config_constants import PATH


def generate_id_column_name(table_name):
    # Remove "dim"/"fct" at the start, "s" at the end, and add "Id" at the end for general case
    if (
        table_name.startswith("dim")
        or table_name.startswith("fct")
        and table_name.endswith("s")
    ):
        base_name = table_name[3:-1]  # Remove "dim"/"fct" and "s"

        # Special handling for names ending in 'ies' -> 'y'
        if base_name.endswith("ie"):
            base_name = base_name[:-2] + "y"

        return snakecase(base_name) + "_id"
    return table_name


def generate_fct_key_column_name(dim_table_name):
    # Remove "dim" at the start, "s" at the end, and add "Id" at the end for general case
    if dim_table_name.startswith("dim") and dim_table_name.endswith("s"):
        base_name = dim_table_name[3:-1]  # Remove "dim" and "s"

        # Special handling for names ending in 'ies' -> 'y'
        if base_name.endswith("ie"):
            base_name = base_name[:-2] + "y"

        return snakecase(base_name) + "_key"
    return dim_table_name


def generate_dim_table_name_from_id_column_name(dim_table_id_column_name):
    # Remove "Id" at the end and add "dim" at the start for general case
    if dim_table_id_column_name.endswith("_id"):
        base_name = dim_table_id_column_name[:-3]  # Remove "_id"

        # Special handling for names ending in 'y' -> 'ies'
        if base_name.endswith("y"):
            base_name = base_name[:-1] + "ie"

        return "dim" + pascalcase(base_name) + "s"
    return dim_table_id_column_name


def generate_bridge_key_column_name(bridge_table_name):
    # Remove "Bridge" at the end and add "Key" at the end for general case
    if bridge_table_name.endswith("Bridge"):
        base_name = bridge_table_name[3:-6]  # Remove "Job" and "Bridge"
        return snakecase(base_name) + "_key"
    return bridge_table_name


def construct_file_path_for_data_source(source_name):
    return os.path.join(
        PATH["data_processed"],
        source_name + "_json",
        source_name + "_cleaned_data.json",
    )


def create_key_name(
    source_name, is_raw=True, search_location=None, search_keyword=None
):
    now = sanitize_string(datetime.now().isoformat(), True)
    location = sanitize_string(search_location) if search_location else ""
    keyword = sanitize_string(search_keyword) if search_keyword else ""
    source = sanitize_string(source_name)

    data_state = "raw" if is_raw else "cleaned"

    parts = [source, data_state, now, location, keyword]
    return "_".join(part for part in parts if part)


def sanitize_string(string, replace=False):
    # note the backslash in front of the "-". otherwise it means from to.
    pattern = "[,!.\-: ]"

    if replace is False:
        filename = re.sub(pattern, "_", string)
    else:
        filename = re.sub(pattern, "", string)

    return filename.lower().replace("__", "_")
