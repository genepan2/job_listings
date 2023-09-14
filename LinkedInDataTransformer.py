# Import necessary libraries
import re
from datetime import datetime
import os
import json

class JobSearchLinkedInTransformer:
    def __init__(self):

        # Ensure the directory exists
        self.directory_path = "json_files/linkedin_json_files"
        if not os.path.exists(self.directory_path):
            os.makedirs(self.directory_path)

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
        # self.print_json(all_data)
        return all_data


    def transform(self, data):

        cleaned_data = []

        for job in data:
            cleaned_job = {key: value.strip() if isinstance(value, str) else value for key, value in job.items()}

            # job_title
            cleaned_job["job_title"] = cleaned_job["job_title"].replace(" (m/f/d)", "").replace(" (f/m/d)", "").replace(" (m/w/d)", "").replace(" (w/m/d)", "")

            # job_amount_applicants
            amount_applicants = re.compile(r'\d+').findall(cleaned_job["job_amount_applicants"])
            cleaned_job["job_amount_applicants"] = amount_applicants[0]

            # job_linkedin_id
            cleaned_job["job_linkedin_id"] = cleaned_job["job_linkedin_id"].replace('<!--', '').replace('-->', '')

            # company_linkedin_url
            cleaned_job["company_linkedin_url"] = cleaned_job["company_linkedin_url"].split('?')[0]

            cleaned_data.append(cleaned_job)

        self.print_json(cleaned_data)


        return cleaned_data

    def clean_filename(self, string, replace = False):
        pattern = "[,!.\-: ]" #note the backslash in front of the "-". otherwise it means from to.

        if replace == False:
            filename = re.sub(pattern, "_", string)
        else:
            filename = re.sub(pattern, "", string)

        return filename.lower().replace("__", "_")

    def create_file_name(self, isRaw = False, type = "json"):
        path = self.directory_path
        # now = self.clean_filename(datetime.now().isoformat(), True)
        # location = self.clean_filename(self.search_location)
        # keyword = self.clean_filename(self.search_keyword)
        # file_number = self.file_number

        # return f"{path}/linkedin_{'raw_' if isRaw else ''}{now}.{type}"
        return f"{path}/linkedin_cleaned_data.json"

    def save_jobs(self, data, type = "json"):
        file_name = self.create_file_name()
        with open(file_name, "w") as json_file:
            json.dump(data, json_file, indent = 4)

    def run_all(self):
        data_raw = self.load()
        data_clean = self.transform(data_raw)
        self.save_jobs(data_clean)


if __name__ == "__main__":
    scraper = JobSearchLinkedInTransformer()
    scraper.run_all()
