from pymongo import MongoClient
import os
import json
import logging

class MongoDBUploader:
    """
    A class to facilitate the upload of JSON data to MongoDB.

    Attributes:
        client: MongoClient object to manage connection to MongoDB.
        db: Reference to the MongoDB database.
        collection: Reference to the MongoDB collection.
        logger: Logger for capturing and reporting runtime information.
    """

    def __init__(self, db_name, collection_name, mongo_uri='mongodb://localhost:27017/'):
        """
        Initializes the MongoDBUploader with a database, collection name, and optionally a MongoDB URI.

        Parameters:
            db_name (str): Name of the MongoDB database.
            collection_name (str): Name of the MongoDB collection.
            mongo_uri (str, optional): MongoDB connection URI. Defaults to localhost.
        """
        if not isinstance(db_name, str) or not isinstance(collection_name, str):
            raise TypeError("Both db_name and collection_name should be strings")

        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
        self.logger = logging.getLogger(__name__)

    def upload_json_file(self, filepath):
        """
        Uploads a specified JSON file to the MongoDB collection.

        Parameters:
            filepath (str): Path to the JSON file to upload.
        """
        try:
            with open(filepath, 'r') as file:
                data = json.load(file)

                if isinstance(data, list):
                    self.collection.insert_many(data)
                else:
                    self.collection.insert_one(data)

                self.logger.info(f"Uploaded {os.path.basename(filepath)} to {self.db.name}.{self.collection.name}")

        except Exception as e:
            self.logger.error(f"An error occurred while uploading {filepath}: {e}")

    def close_connection(self):
        """
        Closes the connection to the MongoDB client.
        """
        self.client.close()
        self.logger.info("MongoDB client connection closed.")

# Example usage (you may want to configure logging before this, or remove these lines)
if __name__ == "__main__":
    uploader = MongoDBUploader("mydatabase", "mycollection")
    uploader.upload_json_file("mydata.json")
    uploader.close_connection()
