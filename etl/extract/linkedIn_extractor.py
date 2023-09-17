import requests
from bs4 import BeautifulSoup
from datetime import datetime
import os
import re
import json

# Define constants
BASE_URL = 'https://www.linkedin.com'
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"
}

class JobSearchLinkedInExtractor:
    def __init__(self, keyword, location, page = 1):
        self.search_keyword = keyword
        self.search_location = location
        self.base_url = f'https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={keyword}&location={location}&currentJobId=3638465660&start={page}'
        self.file_number = 1
        self.job_number = 1
        self.filtered_jobs_buffer = []

        # Ensure the directory exists
        self.directory_path = "data/raw/linkedin_json_files"
        if not os.path.exists(self.directory_path):
            os.makedirs(self.directory_path)

    def scrape_jobs(self):
        job_ids = self.get_job_ids(self.search_keyword, self.search_location)

        search = {
            "keyword": self.search_keyword,
            "location": self.search_location
        }

        job_details = [self.get_job_details(job_id, search) for job_id in job_ids]

        self.save_jobs(job_details, "json")

    # Function to get job IDs from LinkedIn for a given keyword
    def get_job_ids(self, keyword, location):
        job_ids = []
        for page in range(0, 5):
            target_url = f'https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={keyword}&location={location}&currentJobId=3638465660&start={page}'
            res = requests.get(target_url, headers=HEADERS)
            soup = BeautifulSoup(res.text, 'html.parser')
            all_jobs_on_this_page = soup.find_all("li")
            job_ids.extend(job.find("div", {"class": "base-card"}).get('data-entity-urn').split(
                ":")[3] for job in all_jobs_on_this_page if job.find("div", {"class": "base-card"}))

        return job_ids[:3]

    def get_job_details(self, job_id, search):
        # print(search)
        target_url = f'https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}'
        resp = requests.get(target_url, headers=HEADERS)
        soup = BeautifulSoup(resp.text, 'html.parser')

        job_title = soup.find("div", {"class": "top-card-layout__entity-info"})
        job_title = job_title.find("a").text if job_title else None

        # level = soup.find("ul", {"class": "description__job-criteria-list"})
        # level = level.find("li").text

        job_linkedin_id = soup.find(
            "code", {"id": "decoratedJobPostingId"}).string

        job_linkedin_url = target_url

        print(job_linkedin_url)

        amount_applicants_text = soup.select_one(
            ".num-applicants__caption").text

        job_criteria_items = soup.find_all(
            "li", {"class": "description__job-criteria-item"})
        seniority_level_text = job_criteria_items[0].select_one(
            ".description__job-criteria-text").text
        employment_type_text = job_criteria_items[1].select_one(
            ".description__job-criteria-text").text
        job_function_text = job_criteria_items[2].select_one(
            ".description__job-criteria-text").text
        industries_text = job_criteria_items[3].select_one(
            ".description__job-criteria-text").text

        description = soup.select_one(".description__text").select_one(
            ".show-more-less-html__markup")
        description_contents = description.text

        company_html = soup.select_one(
            ".topcard__org-name-link")
        company_name = company_html.string if company_html else None

        company_linkedin_url = company_html['href']

        job_location_html = soup.select_one(
            ".topcard__flavor-row").select_one(".topcard__flavor--bullet")
        job_location = job_location_html.text if job_location_html else None

        return {
            "company_name": company_name,
            "company_linkedin_url": company_linkedin_url,
            "job_title": job_title,
            # "job_level": level,
            "job_location": job_location,
            "job_linkedin_id": job_linkedin_id,
            "job_linkedin_url": job_linkedin_url,
            "job_amount_applicants": amount_applicants_text,
            "job_seniority_level_text": seniority_level_text,
            "job_employment_type_text": employment_type_text,
            "job_job_function_text": job_function_text,
            "job_industries_text": industries_text,
            "job_description": description_contents,
            "search_datetime": datetime.now().isoformat(),
            "search_keyword": search["keyword"],
            "search_location": search["location"]
        }

    def clean_filename(self, string, replace = False):
        pattern = "[,!.\-: ]" #note the backslash in front of the "-". otherwise it means from to.
        print(string)
        if replace == False:
            filename = re.sub(pattern, "_", string)
        else:
            filename = re.sub(pattern, "", string)

        return filename.lower().replace("__", "_")

    def create_file_name(self, isRaw = False, type = "json"):
        path = self.directory_path
        now = self.clean_filename(datetime.now().isoformat(), True)
        location = self.clean_filename(self.search_location)
        keyword = self.clean_filename(self.search_keyword)
        file_number = self.file_number

        return f"{path}/linkedin_{'raw_' if isRaw else ''}{now}_{location}_{keyword}_{file_number}.{type}"

    def save_jobs(self, data, type = "json"):
        file_name = self.create_file_name(True)
        with open(file_name, "w") as json_file:
            json.dump(data, json_file, indent = 4)


####################################

if __name__ == "__main__":
    scraper = JobSearchLinkedInExtractor("Data Engineering", "Cologne, Germany")
    scraper.scrape_jobs()

