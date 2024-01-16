import json
from datetime import datetime
import os
from common.JobListings.constants import PATH, FIELDS
import common.JobListings.HelperTransform as HelperTransform


class ThemuseDataTransformer:
    def __init__(self):
        self.directory_path = os.path.join(PATH['data_raw'], 'themuse_json')
        self.processed_directory_path = os.path.join(
            PATH['data_processed'], 'themuse_json')
        self.job_number = 1

        # Ensure the processed directory exists
        if not os.path.exists(self.processed_directory_path):
            os.makedirs(self.processed_directory_path)

    def transform_job_listing(self, job):

        job_description = HelperTransform.transform_strip_html_tags(
            job.get("contents", ""))

        transformed_job = {
            FIELDS["title"]: HelperTransform.transform_job_title(job.get("name", "")),
            FIELDS["company_name"]: job.get("company", {}).get("name", ""),
            FIELDS["location"]: HelperTransform.transform_job_location(job.get("locations", [{}])[0].get("name", "")) if job.get("locations") else "",
            FIELDS["search_keyword"]: job.get("categories", [{}])[0].get("name", "") if job.get("categories") else "",
            FIELDS["level"]: HelperTransform.transform_job_level(job_title=job.get("name", "")),
            FIELDS["publish_date"]: HelperTransform.transform_to_isoformat(job.get("publication_date", ""), datetime.now().isoformat()),
            FIELDS["url"]: job.get("refs", {}).get("landing_page", ""),
            FIELDS["search_datetime"]: datetime.now().isoformat(),
            FIELDS["description"]: job_description,
            FIELDS["language"]: HelperTransform.transform_detect_language(
                job_description)
        }
        return transformed_job

    def transform_jobs(self):
        with open(f"{self.directory_path}/themuse_raw_data.json", 'r') as raw_file:
            raw_jobs = json.load(raw_file)

        transformed_jobs = []
        for job in raw_jobs:
            job_location = job.get("locations", [{}])[0].get("name", "")
            if "Germany" in job_location or "Flexible / Remote" in job_location:
                job["contents"] = HelperTransform.transform_translate_unicode_characters(
                    job.get("contents", ""))
                transformed_job = self.transform_job_listing(job)
                transformed_jobs.append(transformed_job)
                self.job_number += 1

        with open(f"{self.processed_directory_path}/themuse_cleaned_data.json", 'w') as json_file:
            json.dump(transformed_jobs, json_file, indent=4)
