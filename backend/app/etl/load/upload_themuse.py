from config.mongodb_connection import MongoDBUploader
from config.constants import MONGO, COLLECTIONS

class ThemuseUploader:
    @staticmethod
    def upload():
        themuse_file_path = "backend/app/data/processed/themuse_json_files/themuse_cleaned_data.json"

        uploader = MongoDBUploader(MONGO["db"], COLLECTIONS["themuse"])
        uploader.upload_json_file(themuse_file_path)
        uploader.close()
