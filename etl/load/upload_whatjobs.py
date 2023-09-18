import settings  # Importing settings will also update the system path

from mongodb_connection import MongoDBUploader

def upload_whatjobs():
    whatjobs_collection_name = "whatjobs_jobs_collected"
    
    uploader_whatjobs = MongoDBUploader("job_listing_db", whatjobs_collection_name)
    uploader_whatjobs.upload_json_file(settings.WHATJOBS_FILE_PATH)  # Use the path from settings
    uploader_whatjobs.close()

# Run upload_whatjobs function if this script is executed
if __name__ == "__main__":
    upload_whatjobs()
