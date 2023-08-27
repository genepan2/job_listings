import requests
import json
import re
import unicodedata
from datetime import datetime
import os

def get_requests_result(url, page_number):
    """
    Make a GET request to the specified URL and handle exceptions.
    """
    try:
        res = requests.get(url + str(page_number))
        res.raise_for_status()
        return res
    except requests.exceptions.RequestException as e:
        raise Exception(f"Requesting page {page_number} failed: {e}")

def get_json_content(res):
    """
    Extract JSON content from the response object.
    """
    return res.json()['results']

def strip_html_tags(text):
    """
    Remove HTML tags from the given text using regular expressions.
    """
    clean_text = re.sub(r'<.*?>', '', text)
    return clean_text

def translate_unicode_characters(text):
    """
    Translate Unicode characters to ASCII or remove them.
    """
    translated_text = ''.join(
        char if ord(char) < 128 else unicodedata.normalize('NFKD', char).encode('ascii', 'ignore').decode('utf-8')
        for char in text
    )
    return translated_text

def transform_job_listing(job, search, job_number):
    """
    Process and transform the job listing to the desired format.
    """
    transformed_job = {
        "job_number": job_number,
        "job_title": job.get("name", ""),
        "company_name": job.get("company", {}).get("name", ""),
        "job_location": job.get("locations", [{}])[0].get("name", "") if job.get("locations") else "",
        "search_keyword": job.get("categories", [{}])[0].get("name", "") if job.get("categories") else "",
        "job_level": job.get("levels", [{}])[0].get("name", "") if job.get("levels") else "",
        "publication_date": job.get("publication_date", ""),
        "job_url": job.get("refs", {}).get("landing_page", ""),
        "search_datetime": datetime.now().isoformat(),
        "search_location": search["location"],
        "job_description": job.get("contents", "")
    }
    return transformed_job

# Fixed category value to "Data and Analytics"
search_keyword = "Data%20and%20Analytics"

# Construct the API URL with the selected category and location
url_themuse = f"https://www.themuse.com/api/public/jobs?category={search_keyword}&location=Berlin%2C%20Germany&page="

search = {
    "keyword": "Data and Analytics",
    "location": "Berlin, Germany"
}

# Ensure the directory exists
directory_path = "json_files"
if not os.path.exists(directory_path):
    os.makedirs(directory_path)

# Loop through a range of pages to retrieve job listings
file_number = 1
job_number = 1  # Global job counter
filtered_jobs_buffer = []  # This buffer will store jobs until it reaches a count of 20

for page_number in range(0, 50):
    try:
        # Make a GET request to retrieve job listings
        res = get_requests_result(url_themuse, page_number)
        json_file = get_json_content(res)

        # Process and transform job listings
        for job in json_file:
            job['contents'] = strip_html_tags(job['contents'])
            job['contents'] = translate_unicode_characters(job['contents'])
            transformed_job = transform_job_listing(job, search, job_number)

            if 'Berlin, Germany' in transformed_job.get('job_location', ''):
                transformed_job['job_number'] = job_number  # Add the job_number attribute
                filtered_jobs_buffer.append(transformed_job)
                job_number += 1  # Increment the global job counter

            # Check if the buffer has reached a count of 20
            if len(filtered_jobs_buffer) == 20:
                # Save the buffered jobs to a file
                json_filename = f"{directory_path}/the_muse_jobs_{file_number}.json"
                with open(json_filename, 'w') as json_file:
                    json.dump(filtered_jobs_buffer, json_file, indent=4)
                print(f"Saved 20 jobs in '{json_filename}'.")

                # Reset the buffer and increment the file number
                filtered_jobs_buffer = []
                file_number += 1

    except Exception as exception:
        print(exception)
        break

# If there are any remaining jobs in the buffer after looping, save them to a final file
if filtered_jobs_buffer:
    json_filename = f"{directory_path}/themuse_jobs_{file_number}.json"
    with open(json_filename, 'w') as json_file:
        json.dump(filtered_jobs_buffer, json_file, indent=4)
    print(f"Saved the remaining {len(filtered_jobs_buffer)} jobs in '{json_filename}'.")
