# Import necessary libraries
import re
from datetime import datetime
import os
import json
from config.constants import FIELDS, JOB_LEVELS, JOB_LOCATIONS
from src.utils.transformations import transform_job_level, transform_job_location, transform_to_isoformat

class JobSearchLinkedInTransformer:
    def __init__(self):

        # Raw data directory
        self.directory_path = "data/raw/linkedin_json_files"
        if not os.path.exists(self.directory_path):
            os.makedirs(self.directory_path)

        # Processed data directory
        self.processed_directory_path = "data/processed/linkedin_json_files"
        if not os.path.exists(self.processed_directory_path):
            os.makedirs(self.processed_directory_path)

    def flatten(self, lst):
        flat_list = []
        for item in lst:
            if isinstance(item, list):
                flat_list.extend(self.flatten(item))
            else:
                flat_list.append(item)
        return flat_list

    def print_json(self, data):
        formatted_json = json.dumps(data, indent=4)
        # print(formatted_json)

    def load(self):
        dir_path = self.directory_path
        json_files = [f for f in os.listdir(dir_path) if f.endswith('.json')]
        all_data = []
        for json_file in json_files:
            with open(os.path.join(dir_path, json_file), 'r') as file:
                data = json.load(file)
                all_data.append(data)
        all_data = self.flatten(all_data)
        return all_data

    def transform(self, data):
        cleaned_data = []
        for job in data:
            cleaned_job = {key: value.strip() if isinstance(value, str) else value for key, value in job.items()}

            cleaned_job[FIELDS["title"]] = cleaned_job[FIELDS["title"]].replace(" (m/f/d)", "").replace(" (f/m/d)", "").replace(" (m/w/d)", "").replace(" (w/m/d)", "") if cleaned_job[FIELDS["title"]] else None

            cleaned_job[FIELDS["level"]] = transform_job_level(cleaned_job[FIELDS["level"]], cleaned_job[FIELDS["title"]]) if cleaned_job[FIELDS["level"]] else JOB_LEVELS["unknown"]
            cleaned_job[FIELDS["location"]] = transform_job_location(cleaned_job[FIELDS["location"]]) if cleaned_job[FIELDS["location"]] else JOB_LOCATIONS["unknown"]
            cleaned_job[FIELDS["publish_date"]] = transform_to_isoformat(cleaned_job[FIELDS["publish_date"]], cleaned_job[FIELDS["search_datetime"]])

            amount_applicants = re.compile(r'\d+').findall(cleaned_job[FIELDS["applicants"]]) if cleaned_job[FIELDS["applicants"]] else [0]
            cleaned_job[FIELDS["applicants"]] = amount_applicants[0]

            cleaned_job[FIELDS["linkedin_id"]] = cleaned_job[FIELDS["linkedin_id"]].replace('<!--', '').replace('-->', '') if cleaned_job[FIELDS["linkedin_id"]] else None

            cleaned_job[FIELDS["company_linkedin_url"]] = cleaned_job[FIELDS["company_linkedin_url"]].split('?')[0] if cleaned_job[FIELDS["company_linkedin_url"]] else None

            cleaned_data.append(cleaned_job)
        return cleaned_data

    def clean_filename(self, string, replace = False):
        pattern = "[,!.\-: ]" #note the backslash in front of the "-". otherwise it means from to.
        if replace == False:
            filename = re.sub(pattern, "_", string)
        else:
            filename = re.sub(pattern, "", string)
        return filename.lower().replace("__", "_")

    def create_file_name(self, isRaw = False, type = "json"):
        path = self.directory_path if isRaw else self.processed_directory_path
        return f"{path}/linkedin_cleaned_data.json"

    def save_jobs(self, data, type = "json"):
        file_name = self.create_file_name(isRaw=False)  # Specify that the data isn't raw
        with open(file_name, "w") as json_file:
            json.dump(data, json_file, indent = 4)

    def run_all(self):
        data_raw = self.load()
        data_clean = self.transform(data_raw)
        self.save_jobs(data_clean)

if __name__ == "__main__":
    scraper = JobSearchLinkedInTransformer()
    scraper.run_all()
