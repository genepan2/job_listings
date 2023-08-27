from pymongo import MongoClient
import os
import json

def upload_json_files_to_mongo(folder_path, db_name, collection_name, mongo_uri='mongodb://localhost:27017/'):
    """
    Upload JSON files from a specific folder to a MongoDB collection.
    
    Parameters:
    - folder_path: Path to the folder containing the JSON files.
    - db_name: Name of the MongoDB database.
    - collection_name: Name of the MongoDB collection where data should be inserted.
    - mongo_uri: MongoDB connection URI (default is 'mongodb://localhost:27017/')
    """

    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]

    # List all files in the folder
    for filename in os.listdir(folder_path):
        if filename.endswith(".json"):
            filepath = os.path.join(folder_path, filename)

            with open(filepath, 'r') as file:
                data = json.load(file)
                
                # Ensure data is a list for bulk insert
                if isinstance(data, list):
                    collection.insert_many(data)
                else:
                    collection.insert_one(data)

            print(f"Uploaded {filename} to {db_name}.{collection_name}")

    client.close()


folder_path = "json_files"
db_name = "job_listing_db"
collection_name = "jobs_collected"

upload_json_files_to_mongo(folder_path, db_name, collection_name)
