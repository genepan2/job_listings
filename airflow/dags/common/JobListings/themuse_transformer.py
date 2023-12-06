import json
import re
import unicodedata
from datetime import datetime
import os
import logging
from common.JobListings.constants import *
from langdetect import detect
import common.JobListings.HelperTransform as HelperTransform
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


class ThemuseDataTransformer:
    def __init__(self):
        self.directory_path = os.path.join(PATH['data_raw'], 'themuse_json')
        self.processed_directory_path = os.path.join(PATH['data_processed'], 'themuse_json')
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
## Transformation Methods
####################################################################################
####################################################################################

def transform_job_level(job_level: str = 'Entry', job_title: str = None):
    job_level = job_level.lower().strip()
    job_title = job_title.lower().strip()

    job_levels_mapping = {
        "intern": ["internship", "intern", "working student"],
        "entry": ["entry level", "entry", "junior"],
        "middle": ["mid level", "mid", "not applicable"],
        "senior": ["senior level", "senior"],
        "lead": ["lead"],
        "head": ["head"],
        "student": ["Working Student", "student", "Werkstudent"]
    }

    for level, level_categories in job_levels_mapping.items():
        if job_level in level_categories:
            return JOB_LEVELS[level]

        for category in level_categories:
            if category in job_title:
                return JOB_LEVELS[level]

    return JOB_LEVELS["middle"]


def transform_job_location(job_location: str):
    # job_location = job_location.lower().split(',')[0].strip()
    job_location = job_location.lower()

    locations_mapping = {
        "berlin": ["berlin"],
        "munich": ["munich", "münchen"],
        "hamburg": ["hamburg"],
        "cologne": ["cologne", "köln"],
        "frankfurt": ["frankfurt"],
        "remote": ["flexible / remote", "remote"]
    }

    for location, location_names in locations_mapping.items():
        if any(name in job_location for name in location_names):
            return JOB_LOCATIONS.get(location)

    return JOB_LOCATIONS["other"]


def transform_to_isoformat(publication_date, search_datetime):
    today_names = ["today", "heute"]
    yesterday_names = ["yesterday", "gestern"]

    # Convert search_datetime to a datetime object at the start
    search_datetime_ob = datetime.strptime(
        search_datetime, '%Y-%m-%dT%H:%M:%S.%f')

    # Check if publication_date is a special keyword like "today" or "yesterday"
    if publication_date and publication_date.lower() in today_names or publication_date is None:
        return search_datetime

    if publication_date and publication_date.lower() in yesterday_names:
        new_date_time_obj = search_datetime_ob - timedelta(days=1)
        return new_date_time_obj.isoformat()

    # Use regular expression to extract the numeric value and unit (hour, day, week, month)
    logging.info(publication_date)
    # Add this check
    if publication_date and isinstance(publication_date, str):
        match = re.match(r'(\d+)\s+(\w+)', publication_date)
        if match:
            value, unit = match.groups()
            value = int(value)
            unit = unit.lower()

            unit = unit[:-1] if unit.endswith('s') else unit

            if unit == "second":
                return (search_datetime_ob - timedelta(seconds=value)).isoformat()
            elif unit == "minute":
                return (search_datetime_ob - timedelta(minutes=value)).isoformat()
            elif unit == "hour":
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
    to_remove_genders = ["(f/m/x)", "(m/f/d)", "(f/m/d)", "(m/w/d)", "(w/m/d)", "(M/W/D)",
                         "m/f/d", "(w/m/x)", "(all genders!)", "(all genders)", "(All Genders)"]
    to_remove_levels = ["(Junior)", "Junior", "(Entry Level)", "(Senior)",
                        "Senior", "(Senior Level)", "Intern", "Working Student"]

    to_remove = to_remove_genders + to_remove_levels

    cleaned_title = title
    for phrase in to_remove:
        cleaned_title = cleaned_title.replace(phrase, '')

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
