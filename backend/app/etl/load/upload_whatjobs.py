from config.mongodb_connection import MongoDBUploader
from config.constants import MONGO, COLLECTIONS

class WhatjobsUploader:
    @staticmethod
    def upload():
        whatjobs_file_path = "data/processed/whatjobs_json_files/whatjobs_cleaned_data.json"

        uploader = MongoDBUploader(MONGO["db"], COLLECTIONS["whatjobs"])
        uploader.upload_json_file(whatjobs_file_path)
        uploader.close()
