import json
import os
import logging
from common.JobListings.constants import PATH, FIELDS
import common.JobListings.HelperTransform as HelperTransform

class WhatjobsDataTransformer:
    def __init__(self):
        self.input_directory = os.path.join(PATH['data_raw'], 'whatjobs_json')
        self.output_filename = os.path.join(PATH['data_processed'], 'whatjobs_json', 'whatjobs_cleaned_data.json')

    def transform_data(self):
        all_jobs = []
        counter = 0

        for filename in os.listdir(self.input_directory):
            if filename.endswith(".json"):
                with open(os.path.join(self.input_directory, filename), "r") as infile:
                    jobs = json.load(infile)

                    for job in jobs:
                        try:
                            job[FIELDS["title"]] = HelperTransform.transform_job_title(job.get("title", ""))
                            job[FIELDS["level"]] = HelperTransform.transform_job_level(job_title = job.get("title", ""))
                            job[FIELDS["location"]] = HelperTransform.transform_job_location(job.get("location", ""))
                            job[FIELDS["publish_date"]] = HelperTransform.transform_to_isoformat(job.get("publish_date", ""), job.get("search_datetime", ""))
                            job[FIELDS["language"]] = HelperTransform.transform_detect_language(job["description"])

                            counter += 1
                            if counter % 50 == 0:
                                logging.info(f"Transformed {counter} jobs...")
                        except Exception as e:
                            logging.info(f"Error in job transformation. Error: {e}")
                            continue

                    all_jobs.extend(jobs)

        # Check if the directory exists, if not, create it
        output_directory = os.path.dirname(self.output_filename)
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)

        with open(self.output_filename, "w") as outfile:
            json.dump(all_jobs, outfile, ensure_ascii=False, indent=4)

        logging.info(f"Transformation finished. {len(all_jobs)} jobs saved in '{self.output_filename}'.")

