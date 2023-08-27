import requests
import json
import re
import unicodedata
from datetime import datetime
import os

class JobSearchTheMuse:
    def __init__(self, search_keyword, location):
        self.search_keyword = search_keyword
        self.location = location
        self.base_url = f"https://www.themuse.com/api/public/jobs?category={self.search_keyword}&location={self.location}&page="
        self.file_number = 1
        self.job_number = 1
        self.filtered_jobs_buffer = []

        # Ensure the directory exists
        self.directory_path = "json_files"
        if not os.path.exists(self.directory_path):
            os.makedirs(self.directory_path)

    def get_requests_result(self, url, page_number):
        try:
            res = requests.get(url + str(page_number))
            res.raise_for_status()
            return res
        except requests.exceptions.RequestException as e:
            raise Exception(f"Requesting page {page_number} failed: {e}")

    def get_json_content(self, res):
        return res.json()['results']

    def strip_html_tags(self, text):
        clean_text = re.sub(r'<.*?>', '', text)
        return clean_text

    def translate_unicode_characters(self, text):
        translated_text = ''.join(
            char if ord(char) < 128 else unicodedata.normalize('NFKD', char).encode('ascii', 'ignore').decode('utf-8')
            for char in text
        )
        return translated_text

    def transform_job_listing(self, job):
        transformed_job = {
            "job_number": self.job_number,
            "job_title": job.get("name", ""),
            "company_name": job.get("company", {}).get("name", ""),
            "job_location": job.get("locations", [{}])[0].get("name", "") if job.get("locations") else "",
            "search_keyword": job.get("categories", [{}])[0].get("name", "") if job.get("categories") else "",
            "job_level": job.get("levels", [{}])[0].get("name", "") if job.get("levels") else "",
            "publication_date": job.get("publication_date", ""),
            "job_url": job.get("refs", {}).get("landing_page", ""),
            "search_datetime": datetime.now().isoformat(),
            "search_location": self.location,
            "job_description": job.get("contents", "")
        }
        return transformed_job

    def scrape_jobs(self):
        for page_number in range(0, 50):
            try:
                res = self.get_requests_result(self.base_url, page_number)
                json_file = self.get_json_content(res)

                for job in json_file:
                    job['contents'] = self.strip_html_tags(job['contents'])
                    job['contents'] = self.translate_unicode_characters(job['contents'])
                    transformed_job = self.transform_job_listing(job)

                    if 'Berlin, Germany' in transformed_job.get('job_location', ''):
                        self.filtered_jobs_buffer.append(transformed_job)
                        self.job_number += 1

                    if len(self.filtered_jobs_buffer) == 20:
                        json_filename = f"{self.directory_path}/the_muse_jobs_{self.file_number}.json"
                        with open(json_filename, 'w') as json_file:
                            json.dump(self.filtered_jobs_buffer, json_file, indent=4)
                        print(f"Saved 20 jobs in '{json_filename}'.")

                        self.filtered_jobs_buffer = []
                        self.file_number += 1

            except Exception as exception:
                print(exception)
                break

        if self.filtered_jobs_buffer:
            json_filename = f"{self.directory_path}/themuse_jobs_{self.file_number}.json"
            with open(json_filename, 'w') as json_file:
                json.dump(self.filtered_jobs_buffer, json_file, indent=4)
            print(f"Saved the remaining {len(self.filtered_jobs_buffer)} jobs in '{json_filename}'.")

if __name__ == "__main__":
    scraper = JobSearchTheMuse("Data%20and%20Analytics", "Berlin%2C%20Germany")
    scraper.scrape_jobs()
