
from config.mongodb_connection import MongoDBUploader

class WhatjobsUploader:
    @staticmethod
    def upload():
        whatjobs_file_path = "data/processed/whatjobs_json_files/whatjobs_cleaned_data.json"
        whatjobs_collection_name = "whatjobs_jobs_collected"
        
        uploader = MongoDBUploader("job_listing_db", whatjobs_collection_name)
        uploader.upload_json_file(whatjobs_file_path)
        uploader.close()
