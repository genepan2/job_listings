import settings  # Import settings first to set up paths

from mongodb_connection import MongoDBUploader

def upload_linkedin():
    linkedin_collection_name = "linkedin_jobs_collected"
    
    uploader_linkedin = MongoDBUploader("job_listing_db", linkedin_collection_name)
    uploader_linkedin.upload_json_file(settings.LINKEDIN_FILE_PATH)  # Use the path from settings
    uploader_linkedin.close()

# Run upload_linkedin function if this script is executed
if __name__ == "__main__":
    upload_linkedin()
