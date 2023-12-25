import os

MISC = {
    "items_per_page": 10
}

PATH_ROOT = '/opt/airflow'

PATH = {
    "root": os.path.join(PATH_ROOT),
    "data": os.path.join(PATH_ROOT, 'data'),
    "data_raw": os.path.join(PATH_ROOT, 'data', 'raw'),
    "data_processed": os.path.join(PATH_ROOT, 'data', 'processed'),
    "ml": os.path.join(PATH_ROOT, 'ml'),
}

JOB_LOCATIONS = {
    "berlin": "Berlin",
    "munich": "Munich",
    "hamburg": "Hamburg",
    "cologne": "Cologne",
    "frankfurt": "Frankfurt",
    "remote": "Remote",
    "other": "Other"
}

JOB_LEVELS = {
    "intern": "Internship",
    "entry": "Entry",
    "middle": "Middle",
    "senior": "Senior",
    "lead": "Lead",
    "head": "Head",
    "student": "Student",
    "unknown": "Unknown"
}

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
