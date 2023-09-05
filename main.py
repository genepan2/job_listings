from job_search_whatjobs import JobScraperWhatjobs
from job_search_themuse import JobSearchTheMuse
from job_search_linkedin import JobSearchLinkedIn
from mongodb_connect import main_upload
from merge_to_all_jobs_list import merge_collections_to_all_jobs_list  # Import the function

def main():
    # Scraping jobs from various platforms
    whatjobs_scraper = JobScraperWhatjobs("data-engineer", "berlin--berlin")
    whatjobs_scraper.scrape_all_pages()

    # Scraping from themuse.com
    themuse_scraper = JobSearchTheMuse("Data%20and%20Analytics", "Berlin%2C%20Germany")
    themuse_scraper.scrape_jobs()

    # Scraping from linkedin.com
    linkedin_scraper = JobSearchLinkedIn("Data Engineering", "Berlin, Germany")
    linkedin_scraper.scrape_jobs()

    # Uploading scraped data to MongoDB
    main_upload()

    # Merging collections
    merge_collections_to_all_jobs_list()  # Call the function to merge the collections

if __name__ == "__main__":
    main()
