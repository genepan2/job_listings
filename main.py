from WhatjobsDataExtractor import WhatjobsDataExtractor
from WhatjobsDataTransformer import WhatjobsDataTransformer
from ThemuseDataExtractor import ThemuseDataExtractor
from ThemuseDataTransformer import ThemuseDataTransformer
from mongodb_connect import main_upload
from merge_to_all_jobs_list import merge_collections_to_all_jobs_list

def main():
    # List of job titles and locations to search for on WhatJobs
    job_titles = ["data", "engineer", "software", "machine"]
    locations = ["berlin--berlin", "cologne", "hamburg--hamburg", "munich"]

    # For each job title and location combination, scrape the job listings from WhatJobs
    for title in job_titles:
        for loc in locations:
            extractor = WhatjobsDataExtractor(title, loc)
            extractor.scrape_all_pages()

    # Transforming scraped data from WhatJobs
    whatjobs_transformer = WhatjobsDataTransformer()
    whatjobs_transformer.transform_data()

    # Scraping and transforming data from themuse.com
    themuse_extractor = ThemuseDataExtractor()
    themuse_extractor.extract_jobs()

    themuse_transformer = ThemuseDataTransformer()
    themuse_transformer.transform_jobs()

    # Uploading scraped data to MongoDB
    main_upload()

    # Merging collections
    merge_collections_to_all_jobs_list()

if __name__ == "__main__":
    main()
