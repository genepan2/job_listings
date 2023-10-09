import pandas as pd
from pymongo import MongoClient
import os

class UploadToMongoDB:
    def __init__(self, mongo_uri='mongodb://localhost:27017/'):
        self.mongo_uri = mongo_uri
        self.client = MongoClient(self.mongo_uri)
        self.db = self.client["job_listing_db"]

    def upload_csv_to_mongodb(self, csv_file_path="/home/gab-ubuntu/001-Repositories/job_listing/data/processed/integrated_data/all_jobs_list.csv"):
        """
        Upload the content of a CSV file to a MongoDB collection.

        Parameters:
        - csv_file_path (str): The file path of the CSV file to be uploaded.

        Returns:
        - None
        """
        # Check if the CSV file exists
        if not os.path.exists(csv_file_path):
            print(f"The file '{csv_file_path}' does not exist.")
            return

        # Load the CSV file into a pandas DataFrame
        df = pd.read_csv(csv_file_path, encoding='utf-8')

        # Convert the DataFrame to a list of dictionaries
        data = df.to_dict(orient='records')

        # Drop the collection if it exists to avoid duplicate entries
        if "all_jobs_list" in self.db.list_collection_names():
            self.db["all_jobs_list"].drop()

        # Insert the data into the 'all_jobs_list' collection
        self.db["all_jobs_list"].insert_many(data)

        # Print the total number of documents inserted
        print(f"Uploaded {len(data)} documents to 'all_jobs_list' collection.")

        self.client.close()

# Run the upload process
if __name__ == "__main__":
    uploader = UploadToMongoDB()
    uploader.upload_csv_to_mongodb()
