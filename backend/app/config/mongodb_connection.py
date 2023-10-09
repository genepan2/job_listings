from pymongo import MongoClient
import os
import json

class MongoDBUploader:
    """
    A class to facilitate the upload of JSON data to MongoDB.

    Attributes:
        client: MongoClient object to manage connection to MongoDB.
        db: Reference to the MongoDB database.
        collection: Reference to the MongoDB collection.
    """

    def __init__(self, db_name, collection_name, mongo_uri='mongodb://localhost:27017/'):
        """
        Initializes the MongoDBUploader with a database, collection name, and optionally a MongoDB URI.

        Parameters:
            db_name (str): Name of the MongoDB database.
            collection_name (str): Name of the MongoDB collection.
            mongo_uri (str, optional): MongoDB connection URI. Defaults to localhost.
        """
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def upload_json_file(self, filepath):
        """
        Uploads a specified JSON file to the MongoDB collection.

        Parameters:
            filepath (str): Path to the JSON file to upload.
        """
        with open(filepath, 'r') as file:
            data = json.load(file)

            # Ensure data is a list for bulk insert
            if isinstance(data, list):
                self.collection.insert_many(data)
            else:
                self.collection.insert_one(data)

        print(f"Uploaded {os.path.basename(filepath)} to {self.db.name}.{self.collection.name}")

    def close(self):
        """
        Closes the connection to the MongoDB client.
        """
        self.client.close()
