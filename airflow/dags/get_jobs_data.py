from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.models.baseoperator import chain, cross_downstream
from airflow.exceptions import AirflowException

import requests
from bs4 import BeautifulSoup
from langdetect import detect
from dateutil.relativedelta import relativedelta
from pymongo import MongoClient
import pymongo
import os
import re
import json
import logging
import unicodedata
import urllib.parse
import random

import joblib
import pandas as pd
import numpy as np
from sklearn.preprocessing import OneHotEncoder

# from config.constants import FIELDS

####################################################################################
####################################################################################
## Airflow Variable
####################################################################################
####################################################################################

keywords_linkedin = json.loads(Variable.get("search_keyword_linkedin"))
locations_linkedin = json.loads(Variable.get("search_location_linkedin"))

keywords_whatjobs = json.loads(Variable.get("search_keyword_whatjobs"))
locations_whatjobs = json.loads(Variable.get("search_location_whatjobs"))

keywords_themuse = json.loads(Variable.get("search_keyword_themuse"))
locations_themuse = json.loads(Variable.get("search_location_themuse"))

jobs_to_load = Variable.get("jobs_to_load", default_var=None)
# jobs_to_load = Variable.get("jobs_to_load")

####################################################################################
####################################################################################
## Contants Definition
####################################################################################
####################################################################################

PATH = '/opt/airflow'
PATH_DATA = os.path.join(PATH, 'data')
PATH_DATA_RAW = os.path.join(PATH_DATA, 'raw')
PATH_DATA_PROCESSED = os.path.join(PATH_DATA, 'processed')
PATH_ML = os.path.join(PATH, 'ml')

WHATJOBS_NUM_PAGES_TO_SCRAPE = 1

# JOBS_TO_GET = 10
# JOBS_TO_GET = int(jobs_to_load or None)

if jobs_to_load == 'None' or not jobs_to_load:
    JOBS_TO_GET = None
else:
    try:
        JOBS_TO_GET = int(jobs_to_load)
    except ValueError:
        raise ValueError(f"Expected 'jobs_to_load' to be an integer or 'None', got: {jobs_to_load}")


COLLECTIONS = {
	"themuse": "jobs_themuse",
	"whatjobs": "jobs_whatjobs",
	"linkedin": "jobs_linkedin",
	"all": os.environ.get('MONGO_COLLECTION_ALL', 'jobs_all')
}

MONGO = {
	"uri": os.environ.get('MONGO_URI', 'mongodb://mongo:27017/'),
	"db": "job_listing_db",
    "conn_id": "jobs_mongodb"
}

JOB_LOCATIONS = {
	"berlin": "Berlin",
	"munich": "Munich",
	"hamburg": "Hamburg",
	"cologne": "Cologne",
	"frankfurt": "Frankfurt",
	"other": "Other",
}

JOB_LEVELS = {
	"Intern": "Internship",
	"Entry": "Entry",
	"Middle": "Middle",
	"Senior": "Senior",
	"unknown": "Unknown",
	"Lead": "Lead",
	"Head": "Head",
	"Student": "Student"
}

FIELDS = {
    "number": "number",
    "title": "title",
    "description": "description",
    "level": "level",
    "location": "location",
    "employment": "employment",
    "publish_date": "publish_date",
    "applicants": "applicants",
    "function": "function",
    "industries": "industries",
    "company_name": "company_name",
    "company_linkedin_url": "company_linkedin_url",
    "linkedin_id": "linkedin_id",
    "url": "url",
    "search_datetime": "search_datetime",
    "search_keyword": "search_keyword",
    "search_location": "search_location",
    "language": "language"
}

####################################################################################
####################################################################################
## Classes Extract, Transform
####################################################################################
####################################################################################

class JobSearchLinkedInExtractor:
    def __init__(self, keyword, location, items = None, page = 1):
        self.items = items
        self.search_keyword = keyword
        self.search_location = location
        self.base_url = f'https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={keyword}&location={location}&currentJobId=3638465660&start={page}'
        self.file_number = 1
        self.job_number = 1
        self.filtered_jobs_buffer = []
        self.headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"
            }


        # Ensure the directory exists
        # self.directory_path = PATH_DATA_RAW + "/linkedin_json"
        self.directory_path = os.path.join(PATH_DATA_RAW, 'linkedin_json')
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
            res = requests.get(target_url, headers=self.headers)
            soup = BeautifulSoup(res.text, 'html.parser')
            all_jobs_on_this_page = soup.find_all("li")
            job_ids.extend(job.find("div", {"class": "base-card"}).get('data-entity-urn').split(
                ":")[3] for job in all_jobs_on_this_page if job.find("div", {"class": "base-card"}))

        return job_ids[:self.items] if self.items else job_ids

    def get_job_details(self, job_id, search):
        # logging.info(search)
        target_url = f'https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}'
        resp = requests.get(target_url, headers=self.headers)
        # logging.info(resp.text)
        soup = BeautifulSoup(resp.text, 'html.parser')

        job_title = soup.find("div", {"class": "top-card-layout__entity-info"})
        job_title = job_title.find("a").text if job_title else None

        job_linkedin_id_elem = soup.find("code", {"id": "decoratedJobPostingId"})
        job_linkedin_id = job_linkedin_id_elem.text if job_linkedin_id_elem is not None else None

        job_linkedin_url = target_url

        logging.info(job_linkedin_url)

        amount_applicants_elem = soup.select_one(".num-applicants__caption")
        amount_applicants = amount_applicants_elem.text if amount_applicants_elem is not None else None

        publish_date_elem = soup.select_one(".posted-time-ago__text")
        publish_date = publish_date_elem.text.strip() if publish_date_elem else None

        logging.info(f"publish_date: {publish_date}")

        job_criteria_items = soup.find_all(
            "li", {"class": "description__job-criteria-item"})

        seniority_level = None
        employment_type = None
        job_function = None
        industries = None

        # Check and assign each field individually, protecting against index errors
        if len(job_criteria_items) > 0:
            seniority_level_element = job_criteria_items[0].select_one(".description__job-criteria-text")
            seniority_level = seniority_level_element.text if seniority_level_element else None

        if len(job_criteria_items) > 1:
            employment_type_element = job_criteria_items[1].select_one(".description__job-criteria-text")
            employment_type = employment_type_element.text if employment_type_element else None

        if len(job_criteria_items) > 2:
            job_function_element = job_criteria_items[2].select_one(".description__job-criteria-text")
            job_function = job_function_element.text if job_function_element else None

        if len(job_criteria_items) > 3:
            industries_element = job_criteria_items[3].select_one(".description__job-criteria-text")
            industries = industries_element.text if industries_element else None


        description = soup.select_one(".description__text")
        if description is not None:
            description_contents = description.select_one(".show-more-less-html__markup").text
        else:
            description_contents = None

        company_html = soup.select_one(
            ".topcard__org-name-link")
        company_name = company_html.string if company_html else None

        company_linkedin_url = company_html['href'] if company_html else None

        job_location_html = soup.select_one(".topcard__flavor-row")
        if job_location_html is not None:
            job_location = job_location_html.select_one(".topcard__flavor--bullet").text
        else:
            job_location = None

        return {
            FIELDS["company_name"]: company_name,
            FIELDS["company_linkedin_url"]: company_linkedin_url,
            FIELDS["title"]: job_title,
            FIELDS["location"]: job_location,
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
            FIELDS["search_location"]: search["location"]
        }

    def clean_filename(self, string, replace = False):
        pattern = "[,!.\-: ]" #note the backslash in front of the "-". otherwise it means from to.
        logging.info(string)
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

class WhatjobsDataExtractor:
    global_job_number = 1   # Class level attribute to ensure each job gets a unique identifier
    all_jobs = []  # Class level attribute to store all jobs across different job titles and locations

    def __init__(self, job_title, location, items=None):
        self.job_title = job_title
        self.location = location
        self.items = items
        self.base_url = f"https://de.whatjobs.com/jobs/{self.job_title}/{self.location}"
        self.num_pages_to_scrape = WHATJOBS_NUM_PAGES_TO_SCRAPE  # The number of pages to scrape for each search query
        self.output_filename = os.path.join(PATH_DATA_RAW, 'whatjobs_json', 'whatjobs_raw_data.json')

        # Create a directory for saving the JSON files, if it doesn't already exist
        directory_path = os.path.join(PATH_DATA_RAW, 'whatjobs_json')
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)

        # If this is the first instance and a previous output file exists, reset the all_jobs list for new data
        if WhatjobsDataExtractor.global_job_number == 1 and os.path.exists(self.output_filename):
            WhatjobsDataExtractor.all_jobs = []

    def scrape_page(self, url):
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        job_listings = soup.find_all("div", class_="searchResultItem")
        job_listings = job_listings[:self.items] if self.items else job_listings
        jobs_data = []

        for job in job_listings:
            title_element = job.find("h2", class_="title")
            title = title_element.text.strip() if title_element else "N/A"

            location_element = job.find("div", class_="posR")
            location = location_element.text.split(' ', 1)[1].strip() if location_element else "N/A"

            company_element = job.find("span", class_="wjIcon24 companyName")
            company = company_element.find_parent('div').text.strip() if company_element else "N/A"

            date_published_element = job.find("span", class_="wjIcon24 jobAge")
            date_published = date_published_element.find_parent('div').text.strip() if date_published_element else "N/A"

            description_element = job.find("span", class_="jDesc")
            job_url = description_element.find_next('a')['href'] if description_element else "N/A"
            full_description = self.get_full_description(job_url)  # Get the full description using the URL

            job_data = {
                #FIELDS["number"]: f"whatjobs-{WhatjobsDataExtractor.global_job_number}",
                FIELDS["company_name"]: company,
                FIELDS["title"]: title,
                FIELDS["location"]: location,
                FIELDS["publish_date"]: date_published,
                FIELDS["description"]: full_description,
                FIELDS["url"]: job_url,
                FIELDS["search_datetime"]: datetime.now().isoformat(),
                FIELDS["search_keyword"]: self.job_title,
                FIELDS["search_location"]: location
            }

            # Adding the job data dictionary to the jobs_data list
            jobs_data.append(job_data)
            WhatjobsDataExtractor.global_job_number += 1  # Incrementing the global job number

        return jobs_data

    def scrape_all_pages(self):
        # Loop through all the pages specified by num_pages_to_scrape
        for page in range(1, self.num_pages_to_scrape + 1):
            page_url = f"{self.base_url}?page={page}"  # Forming the URL for each page
            jobs_on_page = self.scrape_page(page_url)  # Scraping the job data from the page

            # If no job data is returned, break the loop
            if not jobs_on_page:
                break

            # Otherwise, extend the all_jobs list with the job data from the page
            WhatjobsDataExtractor.all_jobs.extend(jobs_on_page)

        # Save the aggregated jobs data to a JSON file
        with open(self.output_filename, "w") as outfile:
            json.dump(WhatjobsDataExtractor.all_jobs, outfile, ensure_ascii=False, indent=4)
        logging.info(f"Saved all {len(WhatjobsDataExtractor.all_jobs)} jobs in '{self.output_filename}'.")

    def get_full_description(self, url):
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        description_div = soup.find("div", class_="dDesc")

        if description_div:
            description = description_div.get_text(separator=' ', strip=True)
            description = description.replace("\n", " ")
            description = unicodedata.normalize("NFC", description)
            return description
        else:
            logging.info(f"Description not found for URL: {url}")
            return "N/A"  # Return "N/A" if the description is not found

class ThemuseDataExtractor:
    def __init__(self, categories:list , locations: list, items: int = None):
        categories_str = "&category=".join([urllib.parse.quote(cat) for cat in categories])
        locations_str = "&location=".join([urllib.parse.quote(loc) for loc in locations])
        self.items = items
        self.base_url = (f"https://www.themuse.com/api/public/jobs?category={categories_str}&location={locations_str}&page=")

        # Ensure the directory exists
        self.directory_path = os.path.join(PATH_DATA_RAW, 'themuse_json')
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

        # if there is an limit on the jobs, let'S shuffle them first
        if self.items:
            random.shuffle(job_buffer)
            job_buffer = job_buffer[:self.items]

        # Save the extracted job data to a JSON file
        with open(f"{self.directory_path}/themuse_raw_data.json", 'w') as json_file:
            json.dump(job_buffer, json_file, indent=4)


####################################################################################
####################################################################################
## Classes Transformer
####################################################################################
####################################################################################

class JobSearchLinkedInTransformer:
    def __init__(self):
        self.directory_path = os.path.join(PATH_DATA_RAW, 'linkedin_json')
        self.processed_directory_path = os.path.join(PATH_DATA_PROCESSED, 'linkedin_json')

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
        logging.info(formatted_json)

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

            cleaned_job[FIELDS["title"]] = transform_job_title(cleaned_job[FIELDS["title"]]) if cleaned_job[FIELDS["title"]] else None
            cleaned_job[FIELDS["level"]] = transform_job_level(cleaned_job[FIELDS["level"]], cleaned_job[FIELDS["title"]]) if cleaned_job[FIELDS["level"]] else JOB_LEVELS["Middle"]
            cleaned_job[FIELDS["location"]] = transform_job_location(cleaned_job[FIELDS["location"]]) if cleaned_job[FIELDS["location"]] else JOB_LOCATIONS["other"]
            cleaned_job[FIELDS["publish_date"]] = transform_to_isoformat(cleaned_job[FIELDS["publish_date"]], cleaned_job[FIELDS["search_datetime"]])

            amount_applicants = re.compile(r'\d+').findall(cleaned_job[FIELDS["applicants"]]) if cleaned_job[FIELDS["applicants"]] else [0]
            cleaned_job[FIELDS["applicants"]] = amount_applicants[0]

            cleaned_job[FIELDS["linkedin_id"]] = cleaned_job[FIELDS["linkedin_id"]].replace('<!--', '').replace('-->', '') if cleaned_job[FIELDS["linkedin_id"]] else None
            cleaned_job[FIELDS["company_linkedin_url"]] = cleaned_job[FIELDS["company_linkedin_url"]].split('?')[0] if cleaned_job[FIELDS["company_linkedin_url"]] else None

            cleaned_job["language"] = transform_detect_language(cleaned_job[FIELDS["description"]])

            cleaned_data.append(cleaned_job)
        return cleaned_data

    def clean_filename(self, string, replace = False):
        pattern = "[,!.\-: ]"
        if replace == False:
            filename = re.sub(pattern, "_", string)
        else:
            filename = re.sub(pattern, "", string)
        return filename.lower().replace("__", "_")

    def create_file_name(self, isRaw = False, type = "json"):
        path = self.directory_path if isRaw else self.processed_directory_path
        return f"{path}/linkedin_cleaned_data.json"

    def save_jobs(self, data, type = "json"):
        file_name = self.create_file_name(isRaw=False)
        with open(file_name, "w") as json_file:
            json.dump(data, json_file, indent = 4)

    def run_all(self):
        data_raw = self.load()
        data_clean = self.transform(data_raw)
        self.save_jobs(data_clean)


class WhatjobsDataTransformer:
    def __init__(self):
        self.input_directory = os.path.join(PATH_DATA_RAW, 'whatjobs_json')
        self.output_filename = os.path.join(PATH_DATA_PROCESSED, 'whatjobs_json', 'whatjobs_cleaned_data.json')

    def transform_data(self):
        all_jobs = []
        counter = 0

        for filename in os.listdir(self.input_directory):
            if filename.endswith(".json"):
                with open(os.path.join(self.input_directory, filename), "r") as infile:
                    jobs = json.load(infile)

                    for job in jobs:
                        try:
                            job[FIELDS["title"]] = transform_job_title(job.get("title", ""))
                            job[FIELDS["level"]] = transform_job_level(job_title = job.get("title", ""))
                            job[FIELDS["location"]] = transform_job_location(job.get("location", ""))
                            job[FIELDS["publish_date"]] = transform_to_isoformat(job.get("publish_date", ""), job.get("search_datetime", ""))
                            job[FIELDS["language"]] = transform_detect_language(job["description"])

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


class ThemuseDataTransformer:
    def __init__(self):
        self.directory_path = os.path.join(PATH_DATA_RAW, 'themuse_json')
        self.processed_directory_path = os.path.join(PATH_DATA_PROCESSED, 'themuse_json')
        self.job_number = 1

        # Ensure the processed directory exists
        if not os.path.exists(self.processed_directory_path):
            os.makedirs(self.processed_directory_path)

    def transform_job_listing(self, job):

        job_description = transform_strip_html_tags(job.get("contents", ""))
        # language = transform_detect_language(job_description)

        transformed_job = {
            #FIELDS["number"]: f"themuse-{self.job_number}",
            FIELDS["title"]: transform_job_title(job.get("name", "")),
            FIELDS["company_name"]: job.get("company", {}).get("name", ""),
            FIELDS["location"]: transform_job_location(job.get("locations", [{}])[0].get("name", "")) if job.get("locations") else "",
            FIELDS["search_keyword"]: job.get("categories", [{}])[0].get("name", "") if job.get("categories") else "",
            FIELDS["level"]: transform_job_level(job_title = job.get("name", "")),
            FIELDS["publish_date"]: transform_to_isoformat(job.get("publication_date", ""), datetime.now().isoformat()),
            FIELDS["url"]: job.get("refs", {}).get("landing_page", ""),
            FIELDS["search_datetime"]: datetime.now().isoformat(),
            FIELDS["description"]: job_description,
            FIELDS["language"]: transform_detect_language(job_description)
        }
        return transformed_job

    def transform_jobs(self):
        with open(f"{self.directory_path}/themuse_raw_data.json", 'r') as raw_file:
            raw_jobs = json.load(raw_file)

        transformed_jobs = []
        for job in raw_jobs:
            job_location = job.get("locations", [{}])[0].get("name", "")
            if "Germany" in job_location or "Flexible / Remote" in job_location:
                job["contents"] = transform_translate_unicode_characters(job.get("contents", ""))
                transformed_job = self.transform_job_listing(job)
                transformed_jobs.append(transformed_job)
                self.job_number += 1

        with open(f"{self.processed_directory_path}/themuse_cleaned_data.json", 'w') as json_file:
            json.dump(transformed_jobs, json_file, indent=4)



####################################################################################
####################################################################################
## Machine Learning Classes
####################################################################################
####################################################################################

class SalaryPredictor:
    def __init__(self, source_name):
        logging.info(source_name)
        # self.model_path = 'backend/app/ml/reglog_model.pkl'
        self.model_path = os.path.join(PATH_ML, 'salary', 'reglog_model.pkl')
        self.source_name = source_name

        # self.job_listing_data_path = 'backend/app/data/processed/integrated_data/all_jobs_list.csv'
        # self.job_listing_data_path = os.path.join(PATH_DATA_PROCESSED, 'integrated_data', 'all_jobs_list.csv')

        # self.X_train_path = 'backend/app/ml/X_train.csv'
        self.X_train_path = os.path.join(PATH_ML, 'salary', 'X_train.csv')

        # Load the model and data when an instance is created
        # self.loaded_model = joblib.load(self.model_path)

        try:
            # Check if the file exists
            if os.path.exists(self.model_path) and os.path.isfile(self.model_path):
                self.loaded_model = joblib.load(self.model_path)
            else:
                raise FileNotFoundError(f"Model file does not exist at the specified path: {self.model_path}")
                # raise AirflowException(str(e))
        except Exception as e:
            # Handle other types of exceptions
            logging.info(f"An error occurred while loading the model: {e}")
            raise AirflowException(str(e))

        # self.job_listing_df = pd.read_csv(self.job_listing_data_path)
        # self.job_listing_df = get_data_from_source_collections(source_name)
        self.job_listing_df = pd.DataFrame(get_data_from_source_collections(source_name))
        # logging.info(type(self.job_listing_df))
        self.X_train = pd.read_csv(self.X_train_path)

    def one_hot_encode_data(self):
        job_listing_selected = self.job_listing_df[['location', 'title', 'language', 'level']].copy()
        job_listing_encoded = pd.get_dummies(job_listing_selected)
        return job_listing_encoded

    def fill_missing_columns(self, job_listing_encoded):
        random_fill_values = np.random.randint(2, size=(len(job_listing_encoded), len(self.X_train.columns)))
        random_fill_df = pd.DataFrame(random_fill_values, columns=self.X_train.columns)
        job_listing_encoded = job_listing_encoded.reindex(columns=self.X_train.columns)
        job_listing_encoded.update(random_fill_df)
        return job_listing_encoded

    def predict_salaries(self, job_listing_encoded):
        predicted_salaries = self.loaded_model.predict(job_listing_encoded)
        return predicted_salaries

    def map_salary_to_class(self, salary):
        if salary == 'Class 1':
            return "Up to 60,000"
        elif salary == 'Class 2':
            return "Up to 80,000"
        elif salary == 'Class 3':
            return "Over 100,000"
        else:
            try:
                return int(salary)
            except ValueError:
                return None

    def predict_and_map_salaries(self, output_path=None):
        logging.info("predict_and_map_salaries")
        job_listing_encoded = self.one_hot_encode_data()
        job_listing_encoded = self.fill_missing_columns(job_listing_encoded)
        predicted_salaries = self.predict_salaries(job_listing_encoded)
        # logging.info('dataframe head - {}'.format(predicted_salaries.to_string()))
        self.job_listing_df['predicted_salary'] = [self.map_salary_to_class(s) for s in predicted_salaries]

        # Prepare bulk update operations
        updates = []
        for index, row in self.job_listing_df.iterrows():
            # This assumes you have an '_id' or some unique identifier in your DataFrame
            filter_doc = {'_id': row['_id']}
            # update_doc = {'$set': row.to_dict()}  # This will update all columns. Restrict to specific fields if necessary.
            update_doc = {'$set': {'predicted_salary': row['predicted_salary']}}
            logging.info(row['predicted_salary'])

            updates.append(pymongo.UpdateOne(filter_doc, update_doc))

        # Perform the bulk update
        if updates:
            # result = collection.bulk_write(updates)
            result = update_bulk_records(updates, self.source_name)

        # Check the outcome (optional)
        if result:
            logging.info(f"Matched counts: {result.matched_count}")
            logging.info(f"Modified counts: {result.modified_count}")

        # output_path = 'backend/app/data/processed/integrated_data/predicted_jobs_list.csv'

        # if output_path:
        #     self.job_listing_df.to_csv(output_path, index=False)

        # logging.info("Prediction completed and saved to", output_path)  # Add completion message

        # return self.job_listing_df


####################################################################################
####################################################################################
## Transformation Methods
####################################################################################
####################################################################################

def transform_job_level(job_level: str = 'Entry', job_title: str = None):
  job_level = job_level.lower().strip()
  job_title = job_title.lower().strip()

  job_levels_mapping = {
      "Intern": ["internship", "intern", "working student"],
      "Entry": ["entry level", "entry", "junior"],
      "Middle": ["mid level", "mid", "not applicable"],
      "Senior": ["senior level", "senior"],
      "Lead": ["lead"],
      "Head": ["head"],
      "Student": ["Working Student", "student", "Werkstudent"]
  }

  for level, level_categories in job_levels_mapping.items():
      if job_level in level_categories:
          return JOB_LEVELS[level]

      for category in level_categories:
          if category in job_title:
              return JOB_LEVELS[level]

  return JOB_LEVELS["Middle"]

def transform_job_location(job_location: str):
  # job_location = job_location.lower().split(',')[0].strip()
  job_location = job_location.lower()

  locations_mapping = {
      "berlin": ["berlin"],
      "munich": ["munich", "münchen"],
      "hamburg": ["hamburg"],
      "cologne": ["cologne", "köln"],
      "anywhere": ["flexible / remote"]
  }

  for location, location_names in locations_mapping.items():
      if any(name in job_location for name in location_names):
          return JOB_LOCATIONS.get(location)

  return JOB_LOCATIONS["other"]


def transform_to_isoformat(publication_date, search_datetime):
    today_names = ["today", "heute"]
    yesterday_names = ["yesterday", "gestern"]

    # Convert search_datetime to a datetime object at the start
    search_datetime_ob = datetime.strptime(search_datetime, '%Y-%m-%dT%H:%M:%S.%f')

    # Check if publication_date is a special keyword like "today" or "yesterday"
    if publication_date and publication_date.lower() in today_names or publication_date is None:
        return search_datetime

    if publication_date and publication_date.lower() in yesterday_names:
        new_date_time_obj = search_datetime_ob - timedelta(days=1)
        return new_date_time_obj.isoformat()

    # Use regular expression to extract the numeric value and unit (hour, day, week, month)
    logging.info(publication_date)
    if publication_date and isinstance(publication_date, str):  # Add this check
        match = re.match(r'(\d+)\s+(\w+)', publication_date)
        if match:
            value, unit = match.groups()
            value = int(value)
            unit = unit.lower()

            unit = unit[:-1] if unit.endswith('s') else unit

            if unit == "second" :
                return (search_datetime_ob - timedelta(seconds=value)).isoformat()
            elif unit == "minute" :
                return (search_datetime_ob - timedelta(minutes=value)).isoformat()
            elif unit == "hour" :
                return (search_datetime_ob - timedelta(hours=value)).isoformat()
            elif unit == "day":
                return (search_datetime_ob - timedelta(days=value)).isoformat()
            elif unit == "week":
                return (search_datetime_ob - timedelta(weeks=value)).isoformat()
            elif unit == "month":
                return (search_datetime_ob - relativedelta(months=value)).isoformat()

    # Attempt to parse the publication_date with different date formats
    date_formats = [
        "%Y-%m-%dT%H:%M:%S.%f",  # ISO format
        "%d.%m.%Y",  # from whatjobs
        "%Y-%m-%dT%H:%M:%SZ",  # from themuse
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
    ]

    for date_format in date_formats:
        try:
            date_obj = datetime.strptime(publication_date, date_format)
            return date_obj.isoformat()
        except ValueError:
            pass  # Try the next format

    # If none of the formats match, raise an exception or return a default value as needed
    raise ValueError("Unable to parse publication_date")

def transform_job_title(title: str):
    to_remove_genders = ["(f/m/x)", "(m/f/d)", "(f/m/d)", "(m/w/d)", "(w/m/d)", "(M/W/D)", "m/f/d", "(w/m/x)", "(all genders!)", "(all genders)", "(All Genders)"]
    to_remove_level = [
        "(Junior)", "Junior", "(Entry Level)"
        "(Senior)", "Senior", "(Senior Level)"
        "Intern",
        "Working Student"
        ]

    title_parts = [part for part in title.split() if part not in to_remove_genders]
    title_parts = [part for part in title_parts if part not in to_remove_level]

    cleaned_title = ' '.join(title_parts)
    cleaned_title = ' '.join(cleaned_title.split())

    return cleaned_title

def transform_detect_language(text):
    try:
        lang = detect(text)
        if lang == 'en':
            return 'English'
        elif lang == 'de':
            return 'German'
        else:
            return 'Other'
    except Exception as e:
        logging.info(f"Error detecting language. Error: {e}")
        return 'Unknown'

def transform_strip_html_tags(text):
    clean_text = re.sub(r'<.*?>', '', text)
    return clean_text

def transform_translate_unicode_characters(text):
    translated_text = ''.join(
        char if ord(char) < 128 else unicodedata.normalize(
            'NFKD', char).encode('ascii', 'ignore').decode('utf-8')
        for char in text
    )
    return translated_text

####################################################################################
####################################################################################
## Database Methods
####################################################################################
####################################################################################

def get_data_from_source_collections(collection_name: str = None):
    all_data = []
    collection_names = get_collection_keys_without_all()

    if collection_name in collection_names:
        collection_names = [collection_name]

    try:
        mongo = MongoHook(conn_id=MONGO["conn_id"])

        for coll_name in collection_names:
            try:
                coll_source = mongo.get_collection(COLLECTIONS[coll_name], MONGO["db"])
                data_source = list(coll_source.find({}))
                if data_source:
                    all_data.extend(data_source)

            except Exception as inner_exception:
                logging.info(f"An error occurred while processing collection '{coll_name}': {inner_exception}")

    except Exception as e:
        logging.info(f"An error occurred while establishing a connection to the database: {e}")

    return all_data


def delete_data_from_source_collections():
    collection_names = get_collection_keys_without_all()
    operation_successful = True

    try:
        mongo = MongoHook(conn_id=MONGO["conn_id"])

        for coll_name in collection_names:
            try:
                coll = mongo.get_collection(COLLECTIONS[coll_name], MONGO["db"])
                coll.delete_many({})

            except Exception as inner_exception:
                logging.info(f"An error occurred while trying to delete documents in collection '{coll_name}': {inner_exception}")
                operation_successful = False  # If any error occurs, mark the operation as unsuccessful

    except Exception as e:
        logging.info(f"An error occurred while establishing a connection to the database: {e}")
        operation_successful = False

    return operation_successful

def load_data_to_collection(collection_name, file_path):
    # Validate inputs
    if collection_name not in COLLECTIONS:
        logging.error(f"Collection {collection_name} is not defined in COLLECTIONS.")
        return False

    if not os.path.exists(file_path):
        logging.error(f"File {file_path} does not exist.")
        return False

    mongo = MongoHook(conn_id=MONGO["conn_id"])
    collection = mongo.get_collection(COLLECTIONS[collection_name], MONGO["db"])

    try:
        with open(file_path, 'r') as file:
            data = json.load(file)

            if isinstance(data, list):
                collection.insert_many(data, ordered=False)
            else:
                collection.insert_one(data)

            logging.info(f"Loaded jobs from {os.path.basename(file_path)} to {MONGO['db']}.{COLLECTIONS[collection_name]}")
            return True  # Successful operation

    except Exception as e:
        logging.error(f"An error occurred while loading data to {collection_name}: {e}")
        return False  # Indicate that the operation was not successful

def update_bulk_records(updates, data_source):
    mongo = MongoHook(conn_id=MONGO["conn_id"])
    collection = mongo.get_collection(COLLECTIONS[data_source], MONGO["db"])

    if updates:
        result = collection.bulk_write(updates)

    return result

####################################################################################
####################################################################################
## Utils Methods
####################################################################################
####################################################################################

def get_collection_keys_without_all():
    filtered_keys = [key for key in COLLECTIONS.keys() if key != "all"]
    return filtered_keys

def construct_file_path_for_data_source(source_name):
    return os.path.join(PATH_DATA_PROCESSED, source_name + '_json', source_name + '_cleaned_data.json')


####################################################################################
####################################################################################
## Tasks Methods
####################################################################################
####################################################################################

def extract_linkedin_jobs():
    for keyword in keywords_linkedin:
        for location in locations_linkedin:
            scraper = JobSearchLinkedInExtractor(keyword, location, JOBS_TO_GET)
            scraper.scrape_jobs()

def extract_whatjobs_jobs():
    for keyword in keywords_whatjobs:
        for location in locations_whatjobs:
            scraper = WhatjobsDataExtractor(keyword, location, JOBS_TO_GET)
            scraper.scrape_all_pages()

def extract_themuse_jobs():
    themuse_extractor = ThemuseDataExtractor(keywords_themuse, locations_themuse, JOBS_TO_GET)
    themuse_extractor.extract_jobs()


def transform_linkedin_jobs():
    transformer = JobSearchLinkedInTransformer()
    transformer.run_all()

def transform_whatjobs_jobs():
    transformer = WhatjobsDataTransformer()
    transformer.transform_data()

def transform_themuse_jobs():
    transformer = ThemuseDataTransformer()
    transformer.transform_jobs()


def load_linkedin_to_mongodb():
    source_name = "linkedin"
    file_path = construct_file_path_for_data_source(source_name)
    load_data_to_collection(source_name, file_path)

def load_themuse_to_mongodb():
    source_name = "themuse"
    file_path = construct_file_path_for_data_source(source_name)
    load_data_to_collection(source_name, file_path)

def load_whatjobs_to_mongodb():
    source_name = "whatjobs"
    file_path = construct_file_path_for_data_source(source_name)
    load_data_to_collection(source_name, file_path)


def run_ml_salary():
    collections = get_collection_keys_without_all()
    for collection in collections:
        predictor = SalaryPredictor(collection)
        predictor.predict_and_map_salaries()


# this should be more or less the last task
def load_records_to_main_collection():
    mongo = MongoHook(conn_id=MONGO["conn_id"])
    coll_destination = mongo.get_collection(COLLECTIONS["all"], MONGO["db"])

    all_data = get_data_from_source_collections()

    if all_data:
        try:
            coll_destination.insert_many(all_data, ordered=False)
        except pymongo.errors.BulkWriteError as e:
            for error in e.details['writeErrors']:
                if error['code'] == 11000:  # Duplicate key error code
                    duplicate_url = error['op'].get('url', 'URL not found in document')
                    logging.info("Duplicate documents url: %s", duplicate_url)


####################################################################################
####################################################################################
## DAG
####################################################################################
####################################################################################

default_args = {
    'owner': 'you',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    dag_id='job_posting_aggregator',
    tags=["jobs", "project"],
    default_args=default_args,
    description='Scrape various sources for job postings',
    # schedule_interval=None,
    schedule_interval=timedelta(hours=2),
    start_date=datetime(2023, 10, 1),
    catchup=False,
)

####################################################################################
####################################################################################
## DAG Tasks
####################################################################################
####################################################################################

# linkedin tasks
t10 = PythonOperator(
    task_id='extract_linkedin',
    python_callable=extract_linkedin_jobs,
    dag=dag,
)

t11 = PythonOperator(
    task_id='transform_linkedin',
    python_callable=transform_linkedin_jobs,
    dag=dag,
)

t12 = PythonOperator(
    task_id='load_linkedin',
    python_callable=load_linkedin_to_mongodb,
    dag=dag,
)

# whatjobs tasks
t20 = PythonOperator(
    task_id='extract_whatjobs',
    python_callable=extract_whatjobs_jobs,
    dag=dag,
)

t21 = PythonOperator(
    task_id='transform_whatjobs',
    python_callable=transform_whatjobs_jobs,
    dag=dag,
)

t22 = PythonOperator(
    task_id='load_whatjobs',
    python_callable=load_whatjobs_to_mongodb,
    dag=dag,
)

# themuse tasks
t30 = PythonOperator(
    task_id='extract_themuse',
    python_callable=extract_themuse_jobs,
    dag=dag,
)

t31 = PythonOperator(
    task_id='transform_themuse',
    python_callable=transform_themuse_jobs,
    dag=dag,
)

t32 = PythonOperator(
    task_id='load_themuse',
    python_callable=load_themuse_to_mongodb,
    dag=dag,
)

# ml tasks
m10 = PythonOperator(
    task_id='predict_salary',
    python_callable=run_ml_salary,
    dag=dag,
)

# last tasks
t90 = PythonOperator(
    task_id='copy_to_main_collection',
    python_callable=load_records_to_main_collection,
    dag=dag,
)

t95 = PythonOperator(
    task_id='delete_source_data_db',
    python_callable=delete_data_from_source_collections,
    dag=dag,
)


####################################################################################
####################################################################################
## Tasks Order
####################################################################################
####################################################################################

# t10 >> t11 >> t12 >> t90
# t20 >> t21 >> t22
# t30 >> t31 >> t32
# t3
# [t10, t20, t30] >> [t11, t21, t31] >> [t12, t22, t32] >> t90
# t11 >> t12 >> t21 >> t22 >> t31 >> t32 >> t90

chain(t10, t11, t12)
chain(t20, t21, t22)
chain(t30, t31, t32)

chain(m10, t90, t95)

cross_downstream([t10, t20, t30], [t11, t21, t31])
cross_downstream([t11, t21, t31], [t12, t22, t32])
cross_downstream([t12, t22, t32], [m10, t90, t95])
# cross_downstream([t12, t22, t32], [m10, t90])


if __name__ == "__main__":
    dag.cli()
