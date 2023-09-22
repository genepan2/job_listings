import settings  # Import settings first to set up paths

from config.mongodb_connection import MongoDBUploader

class LinkedinUploader:
    @staticmethod
    def upload():
        linkedin_file_path = "data/processed/linkedin_json_files/linkedin_cleaned_data.json"
        linkedin_collection_name = "linkedin_jobs_collected"
        
        uploader = MongoDBUploader("job_listing_db", linkedin_collection_name)
        uploader.upload_json_file(linkedin_file_path)
        uploader.close()
