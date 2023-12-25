import os

from common.JobListings.constants import PATH, COLLECTIONS


def get_collection_keys_without_all():
    filtered_keys = [key for key in COLLECTIONS.keys() if key != "all"]
    return filtered_keys


def construct_file_path_for_data_source(source_name):
    return os.path.join(PATH["data_processed"], source_name + '_json', source_name + '_cleaned_data.json')
