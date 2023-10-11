from config.mongodb_connection import MongoDBUploader
from config.constants import MONGO, COLLECTIONS

class LinkedinUploader:
    @staticmethod
    def upload():
        linkedin_file_path = "backend/app/data/processed/linkedin_json_files/linkedin_cleaned_data.json"

        uploader = MongoDBUploader(MONGO["db"], COLLECTIONS["linkedin"])
        uploader.upload_json_file(linkedin_file_path)
        uploader.close_connection()
