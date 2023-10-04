import argparse

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


def min_max_cats(x):
    x = int(x)
    if x < 1 or x > 4:
        raise argparse.ArgumentTypeError(f"{x} is an invalid value for cats, must be in range 1-4")
    return x

def min_max_locations(x):
    x = int(x)
    if x < 1 or x > 4:
        raise argparse.ArgumentTypeError(f"{x} is an invalid value for locations, must be in range 1-4")
    return x

def main():
    parser = argparse.ArgumentParser(description='Run specific extractors and transformers.')
    parser.add_argument('--whatjobs', action='store_true', help='Run WhatJobs extraction and transformation')
    parser.add_argument('--themuse', action='store_true', help='Run Themuse extraction and transformation')
    parser.add_argument('--linkedin', action='store_true', help='Run LinkedIn extraction and transformation')

    parser.add_argument('--action', type=str, help='transform, extract or load')

    parser.add_argument('--locations', type=min_max_locations, default=2, help='Amount of locations to process')
    parser.add_argument('--cats', type=min_max_cats, default=2, help='Amount of categories to process')
    parser.add_argument('--jobs', type=int, default=5, help='Amount of jobs (pro cat+location) to process')

    args = parser.parse_args()

    # Default behavior: If none of the three arguments were entered, set all to True
    if not (args.whatjobs or args.themuse or args.linkedin):
        args.whatjobs = args.themuse = args.linkedin = True

    # Extraction, transformation and upload to mongoDB for WhatJobs
    if args.whatjobs:
        job_titles = ["data", "engineer", "software", "machine"]
        job_locations = ["berlin--berlin", "cologne", "hamburg--hamburg", "munich"]
        for title in job_titles if not(args.cats) else job_titles[:args.cats]:
            for loc in job_locations if not(args.locations) else job_locations[:args.locations]:
                whatjobs_extractor = WhatjobsDataExtractor(title, loc, args.jobs)
                whatjobs_extractor.scrape_all_pages()
        whatjobs_transformer = WhatjobsDataTransformer()
        whatjobs_transformer.transform_data()
        WhatjobsUploader.upload()

    # Extraction, transformation and upload to mongoDB for Themuse
    if args.themuse:
        job_titles = ["Computer and IT", "Data and Analytics", "Data Science", "IT", "Science and Engineering", "Software Engineer", "Software Engineering"]
        if args.cats:
            job_titles = job_titles[:args.cats]

        job_locations = ["Berlin, Germany", "Munich, Germany", "Hamburg, Germany", "Cologne, Germany"]
        if args.locations:
            job_locations = job_locations[:args.locations]

        jobs = args.jobs * args.cats * args.locations if args.jobs else None
        themuse_extractor = ThemuseDataExtractor(job_titles, job_locations, jobs)
        themuse_extractor.extract_jobs()
        themuse_transformer = ThemuseDataTransformer()
        themuse_transformer.transform_jobs()
        ThemuseUploader.upload()

    # Extraction, transformation and upload to mongoDB for LinkedIn
    if args.linkedin:
        if args.action == 'extract':
            job_titles = ["Data Engineer", "Big Data Engineer", "Business Intelligence Engineer", "Machine Learning Engineer"]
            job_locations = ["Berlin, Germany", "Munich, Germany", "Hamburg, Germany", "Cologne, Germany"]
            for title in job_titles if not(args.cats) else job_titles[:args.cats]:
                for loc in job_locations if not(args.locations) else job_locations[:args.locations]:
                    linkedin_extractor = JobSearchLinkedInExtractor(title, loc, args.jobs)
                    linkedin_extractor.scrape_jobs()
        elif args.action == 'transform':
            linkedin_transformer = JobSearchLinkedInTransformer()
            linkedin_transformer.run_all()
        else:
            LinkedinUploader.upload()

    # Merging collections in MongoDB
    MergeCollections.merge_to_all_jobs_list()

if __name__ == "__main__":
    main()
