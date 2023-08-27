from job_search_whatjobs import JobScraperWhatjobs
from job_search_themuse import JobSearchTheMuse
from mongodb_connect import MongoDBUploader

def main():
    # Scraping from whatjobs.com
    whatjobs_scraper = JobScraperWhatjobs("data-engineer", "berlin--berlin")
    whatjobs_scraper.scrape_all_pages()
    
    # Scraping from themuse.com
    themuse_scraper = JobSearchTheMuse("Data%20and%20Analytics", "Berlin%2C%20Germany")
    themuse_scraper.scrape_jobs()

    # Uploading to MongoDB
    folder_path = "json_files"
    db_name = "job_listing_db"
    collection_name = "jobs_collected"
    
    uploader = MongoDBUploader(db_name, collection_name)
    uploader.upload_json_files(folder_path)
    uploader.close()

if __name__ == "__main__":
    main()
