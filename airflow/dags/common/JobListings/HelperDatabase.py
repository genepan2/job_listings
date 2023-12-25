from airflow.providers.mongo.hooks.mongo import MongoHook
import pymongo

import os
import json
import logging

import common.JobListings.HelperUtils as HelperUtils

# from constants import PATH, JOB_LOCATIONS, JOB_LEVELS, COLLECTIONS, FIELDS
from common.JobListings.constants import COLLECTIONS, MONGO


def get_data_from_source_collections(collection_name: str = None):
    all_data = []
    collection_names = HelperUtils.get_collection_keys_without_all()

    if collection_name in collection_names:
        collection_names = [collection_name]

    try:
        mongo = MongoHook(conn_id=MONGO["conn_id"])

        for coll_name in collection_names:
            try:
                coll_source = mongo.get_collection(
                    COLLECTIONS[coll_name], MONGO["db"])
                data_source = list(coll_source.find({}))
                if data_source:
                    all_data.extend(data_source)

            except Exception as inner_exception:
                logging.info(
                    f"An error occurred while processing collection '{coll_name}': {inner_exception}")

    except Exception as e:
        logging.info(
            f"An error occurred while establishing a connection to the database: {e}")

    return all_data


def delete_data_from_source_collections():
    collection_names = HelperUtils.get_collection_keys_without_all()
    operation_successful = True

    try:
        mongo = MongoHook(conn_id=MONGO["conn_id"])

        for coll_name in collection_names:
            try:
                coll = mongo.get_collection(
                    COLLECTIONS[coll_name], MONGO["db"])
                coll.delete_many({})

            except Exception as inner_exception:
                logging.info(
                    f"An error occurred while trying to delete documents in collection '{coll_name}': {inner_exception}")
                # If any error occurs, mark the operation as unsuccessful
                operation_successful = False

    except Exception as e:
        logging.info(
            f"An error occurred while establishing a connection to the database: {e}")
        operation_successful = False

    return operation_successful


def load_data_to_collection(collection_name, file_path):
    # Validate inputs
    if collection_name not in COLLECTIONS:
        logging.error(
            f"Collection {collection_name} is not defined in COLLECTIONS.")
        return False

    if not os.path.exists(file_path):
        logging.error(f"File {file_path} does not exist.")
        return False

    mongo = MongoHook(conn_id=MONGO["conn_id"])
    collection = mongo.get_collection(
        COLLECTIONS[collection_name], MONGO["db"])

    try:
        with open(file_path, 'r') as file:
            data = json.load(file)

            if isinstance(data, list):
                collection.insert_many(data, ordered=False)
            else:
                collection.insert_one(data)

            logging.info(
                f"Loaded jobs from {os.path.basename(file_path)} to {MONGO['db']}.{COLLECTIONS[collection_name]}")
            return True  # Successful operation

    except Exception as e:
        logging.error(
            f"An error occurred while loading data to {collection_name}: {e}")
        return False  # Indicate that the operation was not successful


def update_bulk_records(updates, data_source):
    mongo = MongoHook(conn_id=MONGO["conn_id"])
    collection = mongo.get_collection(
        COLLECTIONS[data_source], MONGO["db"])

    if updates:
        result = collection.bulk_write(updates)

    return result


def load_records_to_main_collection(source: str = None):
    mongo = MongoHook(conn_id=MONGO["conn_id"])
    coll_destination = mongo.get_collection(COLLECTIONS["all"], MONGO["db"])

    all_data = get_data_from_source_collections(source)

    if all_data:
        try:
            coll_destination.insert_many(all_data, ordered=False)
        except pymongo.errors.BulkWriteError as e:
            for error in e.details['writeErrors']:
                if error['code'] == 11000:  # Duplicate key error code
                    duplicate_url = error['op'].get(
                        'url', 'URL not found in document')
                    logging.info("Duplicate documents url: %s", duplicate_url)
