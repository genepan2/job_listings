import os

MISC = {
	"items_per_page": 10
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

COLLECTIONS = {
	"themuse": "jobs_themuse",
	"whatjobs": "jobs_whatjobs",
	"linkedin": "jobs_linkedin",
	# "all": os.environ["MONGO_COLLECTION_ALL"]
	"all": os.environ.get('MONGO_COLLECTION_ALL', 'jobs_all')
}

MONGO = {
	# "uri": 'mongodb://localhost:27017/',
	"uri": os.environ.get('MONGO_URI', 'mongodb://localhost:27017/'),
	# "db": os.environ["MONGO_DB_NAME"]
	"db": os.environ.get('MONGO_DB_NAME', 'job_listing_db')
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