from pymongo import MongoClient
import os
import json

class MongoDBUploader:
    def __init__(self, db_name, collection_name, mongo_uri='mongodb://localhost:27017/'):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def upload_json_files(self, folder_path):
        # List all files in the folder
        for filename in os.listdir(folder_path):
            if filename.endswith(".json"):
                filepath = os.path.join(folder_path, filename)

                with open(filepath, 'r') as file:
                    data = json.load(file)
                    
                    # Ensure data is a list for bulk insert
                    if isinstance(data, list):
                        self.collection.insert_many(data)
                    else:
                        self.collection.insert_one(data)

                print(f"Uploaded {filename} to {self.db.name}.{self.collection.name}")

    def close(self):
        self.client.close()

# Paths embedded within the script
def main_upload():
    # For WhatJobs
    whatjobs_folder_path = "json_files/whatjobs_json_files"
    whatjobs_collection_name = "whatjobs_jobs_collected"
    uploader_whatjobs = MongoDBUploader("job_listing_db", whatjobs_collection_name)
    uploader_whatjobs.upload_json_files(whatjobs_folder_path)
    uploader_whatjobs.close()

    # For TheMuse
    themuse_folder_path = "json_files/themuse_json_files"
    themuse_collection_name = "themuse_jobs_collected"
    uploader_themuse = MongoDBUploader("job_listing_db", themuse_collection_name)
    uploader_themuse.upload_json_files(themuse_folder_path)
    uploader_themuse.close()

# If you want to run this directly
if __name__ == "__main__":
    main_upload()
