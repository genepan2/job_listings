import requests
from bs4 import BeautifulSoup
import logging
from datetime import datetime
import pandas as pd
from time import perf_counter
import random
import time

from job_config_constants import FIELDS

# from job_helper_storage import store_df_to_s3
from job_helper_utils import create_key_name
from job_data_storage import JobDataStorage
from job_s3_client_manager import JobS3ClientManager

# SOURCE_NAME = "linkedin"


class JobExtractorLinkedIn:
    def __init__(self, keyword, location, items=None, page=1):
        self.source = "LinkedIn"
        self.items = items
        self.search_keyword = keyword
        self.search_location = location
        # self.base_url = f'https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={keyword}&location={location}&currentJobId=3638465660&start={page}'
        self.base_url = f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={keyword}&location={location}&currentJobId=3791051102&start={page}"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"
        }
        self.s3_manager = JobS3ClientManager()

    def scrape_jobs(self):
        job_ids = self.get_job_ids(self.search_keyword, self.search_location)

        search = {"keyword": self.search_keyword, "location": self.search_location}

        job_details = [self.get_job_details(job_id, search) for job_id in job_ids]

        df = pd.DataFrame(job_details)

        key_name = create_key_name(
            self.source, True, self.search_location, self.search_keyword
        )
        bucket = "bronze"
        logging.info(key_name)
        df.info()
        logging.info(df.isnull().sum())

        s3_client = self.s3_manager.get_boto_client()
        JobDataStorage.save_df_to_s3_as_csv(
            s3_client, df, bucket, self.source, key_name
        )
        # store_df_to_s3(df, file_name, bucket)

    # Function to get job IDs from LinkedIn for a given keyword
    def get_job_ids(self, keyword, location):
        job_ids = []
        for page in range(0, 5):
            target_url = f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={keyword}&location={location}&currentJobId=3638465660&start={page}"
            res = requests.get(target_url, headers=self.headers)
            soup = BeautifulSoup(res.text, "html.parser")
            all_jobs_on_this_page = soup.find_all("li")
            job_ids.extend(
                job.find("div", {"class": "base-card"})
                .get("data-entity-urn")
                .split(":")[3]
                for job in all_jobs_on_this_page
                if job.find("div", {"class": "base-card"})
            )

        return job_ids[: self.items] if self.items else job_ids

    def get_job_details(self, job_id, search):
        pause_random = random.uniform(0.75, 2)
        time.sleep(pause_random)
        start_time_scraping = perf_counter()
        # logging.info(search)
        target_url = f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"
        resp = requests.get(target_url, headers=self.headers)
        # logging.info(resp.text)
        end_time_scraping = perf_counter()
        total_time_scraping = end_time_scraping - start_time_scraping
        soup = BeautifulSoup(resp.text, "html.parser")

        job_title = soup.find("div", {"class": "top-card-layout__entity-info"})
        job_title = job_title.find("a").text.strip() if job_title else None

        # job_linkedin_id_elem = soup.find(
        # "code", {"id": "decoratedJobPostingId"})
        # job_linkedin_id = job_linkedin_id_elem.text if job_linkedin_id_elem is not None else None

        job_linkedin_id = job_id

        job_linkedin_url = target_url

        amount_applicants_elem = soup.select_one(".num-applicants__caption")
        amount_applicants = (
            amount_applicants_elem.text.strip()
            if amount_applicants_elem is not None
            else None
        )

        publish_date_elem = soup.select_one(".posted-time-ago__text")
        publish_date = publish_date_elem.text.strip() if publish_date_elem else None

        job_criteria_items = soup.find_all(
            "li", {"class": "description__job-criteria-item"}
        )

        seniority_level = None
        employment_type = None
        job_function = None
        industries = None

        # Check and assign each field individually, protecting against index errors
        if len(job_criteria_items) > 0:
            seniority_level_element = job_criteria_items[0].select_one(
                ".description__job-criteria-text"
            )
            seniority_level = (
                seniority_level_element.text.strip()
                if seniority_level_element
                else None
            )

        if len(job_criteria_items) > 1:
            employment_type_element = job_criteria_items[1].select_one(
                ".description__job-criteria-text"
            )
            employment_type = (
                employment_type_element.text.strip()
                if employment_type_element
                else None
            )

        if len(job_criteria_items) > 2:
            job_function_element = job_criteria_items[2].select_one(
                ".description__job-criteria-text"
            )
            job_function = (
                job_function_element.text.strip() if job_function_element else None
            )

        if len(job_criteria_items) > 3:
            industries_element = job_criteria_items[3].select_one(
                ".description__job-criteria-text"
            )
            industries = industries_element.text.strip() if industries_element else None

        description = soup.select_one(".description__text")
        if description is not None:
            description_contents = description.select_one(
                ".show-more-less-html__markup"
            ).text.strip()
        else:
            description_contents = None

        company_html = soup.select_one(".topcard__org-name-link")
        company_name = company_html.string.strip() if company_html else None

        company_linkedin_url = company_html["href"] if company_html else None

        job_location_html = soup.select_one(".topcard__flavor-row")
        if job_location_html is not None:
            job_location = job_location_html.select_one(
                ".topcard__flavor--bullet"
            ).text.strip()
        else:
            job_location = None

        job_country = (
            search["location"].split(",")[1].strip()
            if "," in search["location"]
            else ""
        )

        return {
            FIELDS["company_name"]: company_name,
            FIELDS["company_linkedin_url"]: company_linkedin_url,
            FIELDS["title"]: job_title,
            FIELDS["location"]: job_location,
            FIELDS["country"]: job_country,
            FIELDS["linkedin_id"]: job_linkedin_id,
            FIELDS["url"]: job_linkedin_url,
            FIELDS["applicants"]: amount_applicants,
            FIELDS["publish_date"]: publish_date,
            FIELDS["level"]: seniority_level,
            FIELDS["employment"]: employment_type,
            FIELDS["function"]: job_function,
            FIELDS["industries"]: industries,
            FIELDS["description"]: description_contents,
            FIELDS["search_datetime"]: datetime.now().isoformat(),
            FIELDS["search_keyword"]: search["keyword"],
            FIELDS["search_location"]: search["location"],
            FIELDS["source"]: self.source,
            FIELDS["scrape_dur_ms"]: total_time_scraping,
            FIELDS["scraping_pause"]: pause_random,
        }
