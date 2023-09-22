import settings  # Import settings first to set up paths

from config.mongodb_connection import MongoDBUploader

class ThemuseUploader:
    @staticmethod
    def upload():
        themuse_file_path = "data/processed/themuse_json_files/themuse_cleaned_data.json"
        themuse_collection_name = "themuse_jobs_collected"
        
        uploader = MongoDBUploader("job_listing_db", themuse_collection_name)
        uploader.upload_json_file(themuse_file_path)
        uploader.close()
