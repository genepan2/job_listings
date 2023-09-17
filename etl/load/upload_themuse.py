import settings  # Import settings first to set up paths

from mongodb_connection import MongoDBUploader

def upload_themuse():
    themuse_collection_name = "themuse_jobs_collected"
    
    uploader_themuse = MongoDBUploader("job_listing_db", themuse_collection_name)
    uploader_themuse.upload_json_file(settings.THEMUSE_FILE_PATH)  # Use the path from settings
    uploader_themuse.close()

# Run upload_themuse function if this script is executed
if __name__ == "__main__":
    upload_themuse()
