import json
import os
import requests
import logging
from bs4 import BeautifulSoup
import unicodedata
from datetime import datetime
from common.JobListings.constants import FIELDS, PATH

# The number of pages to be scraped from each search result
WHATJOBS_NUM_PAGES_TO_SCRAPE = 1


class ExtractorWhatjobs:
    # Class level attribute to ensure each job gets a unique identifier
    global_job_number = 1
    all_jobs = []  # Class level attribute to store all jobs across different job titles and locations

    def __init__(self, job_title, location, items=None):
        self.job_title = job_title
        self.location = location
        self.items = items
        self.base_url = f"https://de.whatjobs.com/jobs/{self.job_title}/{self.location}"
        # The number of pages to scrape for each search query
        self.num_pages_to_scrape = WHATJOBS_NUM_PAGES_TO_SCRAPE
        self.output_filename = os.path.join(
            PATH['data_raw'], 'whatjobs_json', 'whatjobs_raw_data.json')

        # Create a directory for saving the JSON files, if it doesn't already exist
        directory_path = os.path.join(PATH['data_raw'], 'whatjobs_json')
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
            location = location_element.text.split(
                ' ', 1)[1].strip() if location_element else "N/A"

            company_element = job.find("span", class_="wjIcon24 companyName")
            company = company_element.find_parent(
                'div').text.strip() if company_element else "N/A"

            date_published_element = job.find("span", class_="wjIcon24 jobAge")
            date_published = date_published_element.find_parent(
                'div').text.strip() if date_published_element else "N/A"

            description_element = job.find("span", class_="jDesc")
            job_url = description_element.find_next(
                'a')['href'] if description_element else "N/A"
            full_description = self.get_full_description(
                job_url)  # Get the full description using the URL

            job_data = {
                # FIELDS["number"]: f"whatjobs-{WhatjobsDataExtractor.global_job_number}",
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
            # Incrementing the global job number
            WhatjobsDataExtractor.global_job_number += 1

        return jobs_data

    def scrape_all_pages(self):
        # Loop through all the pages specified by num_pages_to_scrape
        for page in range(1, self.num_pages_to_scrape + 1):
            # Forming the URL for each page
            page_url = f"{self.base_url}?page={page}"
            # Scraping the job data from the page
            jobs_on_page = self.scrape_page(page_url)

            # If no job data is returned, break the loop
            if not jobs_on_page:
                break

            # Otherwise, extend the all_jobs list with the job data from the page
            WhatjobsDataExtractor.all_jobs.extend(jobs_on_page)

        # Save the aggregated jobs data to a JSON file
        with open(self.output_filename, "w") as outfile:
            json.dump(WhatjobsDataExtractor.all_jobs,
                      outfile, ensure_ascii=False, indent=4)
        logging.info(
            f"Saved all {len(WhatjobsDataExtractor.all_jobs)} jobs in '{self.output_filename}'.")

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
