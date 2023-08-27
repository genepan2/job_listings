from pymongo import MongoClient
import os
import json

class MongoDBUploader:
    def __init__(self, db_name, collection_name, mongo_uri='mongodb://localhost:27017/'):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def upload_json_files(self, folder_path):
        """
        Upload JSON files from a specific folder to the MongoDB collection.
        
        Parameters:
        - folder_path: Path to the folder containing the JSON files.
        """
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


# If you want to run this directly
if __name__ == "__main__":
    folder_path = "json_files"
    db_name = "job_listing_db"
    collection_name = "jobs_collected"
    
    uploader = MongoDBUploader(db_name, collection_name)
    uploader.upload_json_files(folder_path)
    uploader.close()
