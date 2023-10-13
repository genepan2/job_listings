from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.providers.mongo.hooks.mongo import MongoHook

import requests
from bs4 import BeautifulSoup
from langdetect import detect
from dateutil.relativedelta import relativedelta
from pymongo import MongoClient
import os
import re
import json
import logging

# from config.constants import FIELDS

############################
## Airflow Variable
############################

keywords_linkedin = json.loads(Variable.get("search_keyword_linkedin"))
locations_linkedin = json.loads(Variable.get("search_location_linkedin"))

############################
## Contants Definition
############################

PATH = '/opt/airflow/data'
PATH_RAW = os.path.join(PATH, 'raw')
PATH_PROCESSED = os.path.join(PATH, 'processed')

COLLECTIONS = {
	"themuse": "themuse_jobs_collected",
	"whatjobs": "whatjobs_jobs_collected",
	"linkedin": "linkedin_jobs_collected",
	"all": "all_jobs_list"
}

MONGO = {
	# "uri": "mongodb://localhost:27017/",
	# "uri": "mongodb://0.0.0.0:27017/",
	"uri": "mongodb://mongo:27017/",
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

############################
## Classes (JobSearchLinkedInExtractor, JobSearchLinkedInTransformer)
############################

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
        # self.directory_path = PATH_RAW + "/linkedin_json"
        self.directory_path = os.path.join(PATH_RAW, 'linkedin_json')
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

        if len(job_criteria_items) > 0:
            seniority_level = job_criteria_items[0].select_one(".description__job-criteria-text").text
            employment_type = job_criteria_items[1].select_one(".description__job-criteria-text").text
            job_function = job_criteria_items[2].select_one(".description__job-criteria-text").text
            industries = job_criteria_items[3].select_one(".description__job-criteria-text").text
        else:
            seniority_level = None
            employment_type = None
            job_function = None
            industries = None

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

class JobSearchLinkedInTransformer:
    def __init__(self):
        # self.directory_path = "backend/app/data/raw/linkedin_json_files"
        # self.processed_directory_path = "backend/app/data/processed/linkedin_json_files"
        self.directory_path = os.path.join(PATH_RAW, 'linkedin_json')
        self.processed_directory_path = os.path.join(PATH_PROCESSED, 'linkedin_json')

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

    def detect_language(self, text):
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
            return 'Other'

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

            # Adding language detection
            cleaned_job["language"] = self.detect_language(cleaned_job[FIELDS["description"]])

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

class MongoDBUploader:

    def __init__(self, db_name, collection_name, mongo_uri=MONGO['uri']):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def upload_json_file(self, filepath):
        with open(filepath, 'r') as file:
            data = json.load(file)

            # Ensure data is a list for bulk insert
            if isinstance(data, list):
                self.collection.insert_many(data)
            else:
                self.collection.insert_one(data)

        print(f"Uploaded {os.path.basename(filepath)} to {self.db.name}.{self.collection.name}")

    def close(self):
        self.client.close()

############################
## Transformation Methods
############################

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
      "cologne": ["cologne", "köln"]
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
    to_remove_level = ["(Junior)", "Junior", "(Senior)", "Senior", "Intern", "Working Student"]

    # Remove gender-related phrases
    title_parts = [part for part in title.split() if part not in to_remove_genders]
    # Remove level-related phrases
    title_parts = [part for part in title_parts if part not in to_remove_level]

    # Join the remaining parts back into a string
    cleaned_title = ' '.join(title_parts)

    # Remove extra spaces and strip
    cleaned_title = ' '.join(cleaned_title.split())

    return cleaned_title


############################
## Tasks Methods
############################

def scrape_linkedin_jobs():
    for keyword in keywords_linkedin:
        for location in locations_linkedin:
            scraper = JobSearchLinkedInExtractor(keyword, location, 2)
            scraper.scrape_jobs()

def transform_linkedin_jobs():
    transformer = JobSearchLinkedInTransformer()
    transformer.run_all()

def upload_linkedin_jobs():
    linkedin_file_path = os.path.join(PATH_PROCESSED, 'linkedin_json', 'linkedin_cleaned_data.json')

    uploader = MongoDBUploader(MONGO["db"], COLLECTIONS["linkedin"])
    uploader.upload_json_file(linkedin_file_path)
    uploader.close()

def load_linkedin_to_mongodb():
    linkedin_file_path = os.path.join(PATH_PROCESSED, 'linkedin_json', 'linkedin_cleaned_data.json')

    mongo = MongoHook(mongo_conn_id=MONGO["conn_id"])
    collection = mongo.get_collection(COLLECTIONS["linkedin"], MONGO["db"])

    # if ("url" not in collection.getIndexes()):
    #     collection.createIndex([("url", 1)], unique=True)

    try:
        with open(linkedin_file_path, 'r') as file:
            data = json.load(file)

            if isinstance(data, list):
                collection.insert_many(data)
            else:
                collection.insert_one(data)

            logging.info(f"Loaded jobs from {os.path.basename(linkedin_file_path)} to {MONGO['db']}.{COLLECTIONS['linkedin']}")

    except Exception as e:
        logging.error(f"An error occurred while loading {linkedin_file_path}: {e}")

############################
## DAG
############################

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
    # schedule_interval=timedelta(days=1),  # This means the DAG will run daily. Adjust as needed.
    # schedule_interval=timedelta(hours=4),  # This means the DAG will run every 4 hour
    schedule_interval=None,  # This means the DAG will run daily. Adjust as needed.
    start_date=datetime(2023, 10, 1),  # Adjust your start date accordingly
    catchup=False,
)

############################
## Tasks
############################

# t1 = PythonOperator(
#     task_id='scrape_linkedin',
#     python_callable=scrape_linkedin_jobs,
#     dag=dag,
# )

# t2 = PythonOperator(
#     task_id='transform_linkedin',
#     python_callable=transform_linkedin_jobs,
#     dag=dag,
# )

t3 = PythonOperator(
    task_id='load_linkedin',
    python_callable=load_linkedin_to_mongodb,
    dag=dag,
)

############################
## Tasks Order
############################

# t1 >> t2 >> t3
t3

# If you had multiple tasks, you'd set their execution order here.
# For now, we only have one task, so no need to set any order.

if __name__ == "__main__":
    dag.cli()
