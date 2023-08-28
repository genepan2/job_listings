from job_search_whatjobs import JobScraperWhatjobs
from job_search_themuse import JobSearchTheMuse
from mongodb_connect import main_upload

def main():
    # Scraping jobs from various platforms
    whatjobs_scraper = JobScraperWhatjobs("data-engineer", "berlin--berlin")
    whatjobs_scraper.scrape_all_pages()

    # Scraping from themuse.com
    themuse_scraper = JobSearchTheMuse("Data%20and%20Analytics", "Berlin%2C%20Germany")
    themuse_scraper.scrape_jobs()

    # Uploading scraped data to MongoDB
    main_upload()

if __name__ == "__main__":
    main()
