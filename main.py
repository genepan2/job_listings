# Import extraction, transformation, and loading functions from their respective locations in the project structure
from etl.extract.whatjobs_extractor import WhatjobsDataExtractor
from etl.transform.whatjobs_transformer import WhatjobsDataTransformer
from etl.load.upload_whatjobs import WhatjobsUploader

from etl.extract.themuse_extractor import ThemuseDataExtractor
from etl.transform.themuse_transformer import ThemuseDataTransformer
from etl.load.upload_themuse import ThemuseUploader

from etl.extract.linkedin_extractor import JobSearchLinkedInExtractor
from etl.transform.linkedin_transformer import JobSearchLinkedInTransformer
from etl.load.upload_linkedin import LinkedinUploader

from src.utils.merge_collections import MergeCollections


def main():
    '''
    # Extraction and transformation for WhatJobs
    job_titles = ["data", "engineer", "software", "machine"]
    locations = ["berlin--berlin", "cologne", "hamburg--hamburg", "munich"]
    for title in job_titles:
        for loc in locations:
            whatjobs_extractor = WhatjobsDataExtractor(title, loc)
            whatjobs_extractor.scrape_all_pages()
    whatjobs_transformer = WhatjobsDataTransformer()
    whatjobs_transformer.transform_data()

    # Extraction and transformation for Themuse
    themuse_extractor = ThemuseDataExtractor()
    themuse_extractor.extract_jobs()
    themuse_transformer = ThemuseDataTransformer()
    themuse_transformer.transform_jobs()


    '''
    # Extraction and transformation for LinkedIn
    # job_titles_linkedin = ["Data Engineer", "Big Data Engineer", "Business Intelligence Engineer", "Machine Learning Engineer"]
    job_titles_linkedin = ["Data Engineer", "Big Data Engineer"]
    # locations_linkedin = ["Berlin, Germany", "Cologne, Germany", "Hamburg, Germany", "Munich, Germany"]
    locations_linkedin = ["Berlin, Germany", "Munich, Germany"]
    for title in job_titles_linkedin:
        for location in locations_linkedin:
            linkedin_extractor = JobSearchLinkedInExtractor(title, location)
            linkedin_extractor.scrape_jobs()
    linkedin_transformer = JobSearchLinkedInTransformer()
    linkedin_transformer.run_all()

    # Uploading to MongoDB
    # WhatjobsUploader.upload()
    # ThemuseUploader.upload()
    LinkedinUploader.upload()

    # Merging collections in MongoDB
    MergeCollections.merge_to_all_jobs_list()

if __name__ == "__main__":
    main()
