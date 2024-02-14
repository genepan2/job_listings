import requests
import json
import os
import urllib.parse
import random
import logging
from job_constants import PATH


class JobExtractorThemuse:
    def __init__(self, categories: list, locations: list, items: int = None):
        categories_str = "&category=".join(
            [urllib.parse.quote(cat) for cat in categories])
        locations_str = "&location=".join(
            [urllib.parse.quote(loc) for loc in locations])
        self.items = items
        self.base_url = (
            f"https://www.themuse.com/api/public/jobs?category={categories_str}&location={locations_str}&page=")
        self.directory_path = os.path.join(PATH['data_raw'], 'themuse_json')
        if not os.path.exists(self.directory_path):
            os.makedirs(self.directory_path)

    def get_requests_result(self, url, page_number):
        try:
            res = requests.get(url + str(page_number))
            res.raise_for_status()
            return res
        except requests.exceptions.RequestException as e:
            raise Exception(f"Requesting page {page_number} failed: {e}")

    def extract_jobs(self):
        job_buffer = []
        for page_number in range(0, 20):
            try:
                res = self.get_requests_result(self.base_url, page_number)
                job_buffer.extend(res.json()['results'])
            except Exception as exception:
                logging.info(exception)
                break

        if self.items:
            random.shuffle(job_buffer)
            job_buffer = job_buffer[:self.items]

        with open(f"{self.directory_path}/themuse_raw_data.json", 'w') as json_file:
            json.dump(job_buffer, json_file, indent=4)
