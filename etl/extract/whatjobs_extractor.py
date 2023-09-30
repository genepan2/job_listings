import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime
import os
from config.constants import FIELDS

# The number of pages to be scraped from each search result
NUM_PAGES_TO_SCRAPE = 1

class WhatjobsDataExtractor:
    """
    Class responsible for extracting job listing data from WhatJobs based on provided job title and location.
    """
    global_job_number = 1   # Class level attribute to ensure each job gets a unique identifier
    all_jobs = []  # Class level attribute to store all jobs across different job titles and locations

    def __init__(self, job_title, location, items = None):
        """
        Initialize the extractor with job title and location.

        Parameters:
        - job_title (str): The job title to search for.
        - location (str): The location to search within.
        """
        self.job_title = job_title
        self.location = location
        self.items = items
        self.base_url = f"https://de.whatjobs.com/jobs/{self.job_title}/{self.location}"
        self.num_pages_to_scrape = NUM_PAGES_TO_SCRAPE
        self.output_filename = "data/raw/whatjobs_json_files/whatjobs_raw_data.json"

        # Create a directory for saving the JSON files, if it doesn't already exist
        directory_path = "data/raw/whatjobs_json_files"
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)

        # If this is the first instance and a previous output file exists, reset the all_jobs list for new data
        if WhatjobsDataExtractor.global_job_number == 1 and os.path.exists(self.output_filename):
            WhatjobsDataExtractor.all_jobs = []

    def scrape_page(self, url):
        """
        Scrape job data from a single page of WhatJobs.

        Parameters:
        - url (str): The URL of the page to scrape.

        Returns:
        - list: A list of dictionaries containing job data.
        """
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        job_listings = soup.find_all("div", class_="searchResultItem")
        job_listings = job_listings[:self.items] if self.items else job_listings
        jobs_data = []

        for job in job_listings:
            # Extract various data elements from the job listing
            title_element = job.find("h2", class_="title")
            title = title_element.text.strip() if title_element else "N/A"

            location_element = job.find("div", class_="posR")
            location = location_element.text.split(' ', 1)[1].strip() if location_element else "N/A"

            company_element = job.find("span", class_="wjIcon24 companyName")
            company = company_element.find_parent('div').text.strip() if company_element else "N/A"

            date_published_element = job.find("span", class_="wjIcon24 jobAge")
            date_published = date_published_element.find_parent('div').text.strip() if date_published_element else "N/A"

            description_element = job.find("span", class_="jDesc")
            description = description_element.text.strip() if description_element else "N/A"
            job_url = description_element.find_next('a')['href'] if description_element else "N/A"

            # Collate the extracted data into a dictionary
            job_data = {
                FIELDS["number"]: f"whatjobs-{WhatjobsDataExtractor.global_job_number}",
                FIELDS["company_name"]: company,
                FIELDS["title"]: title,
                FIELDS["location"]: location,
                FIELDS["publish_date"]: date_published,
                FIELDS["description"]: description,
                FIELDS["url"]: job_url,
                FIELDS["search_datetime"]: datetime.now().isoformat(),
                FIELDS["search_keyword"]: self.job_title,
                FIELDS["search_location"]: location
            }

            jobs_data.append(job_data)
            WhatjobsDataExtractor.global_job_number += 1

        return jobs_data

    def scrape_all_pages(self):
        """
        Scrape job data from all pages of WhatJobs based on the initialized job title and location.
        """
        for page in range(1, self.num_pages_to_scrape + 1):
            page_url = f"{self.base_url}?page={page}"
            jobs_on_page = self.scrape_page(page_url)

            if not jobs_on_page:
                break

            WhatjobsDataExtractor.all_jobs.extend(jobs_on_page)

        # Save the aggregated jobs data to a JSON file
        with open(self.output_filename, "w") as outfile:
            json.dump(WhatjobsDataExtractor.all_jobs, outfile, ensure_ascii=False, indent=4)
        print(f"Saved all {len(WhatjobsDataExtractor.all_jobs)} jobs in '{self.output_filename}'.")

if __name__ == "__main__":
    # List of job titles and locations to search for
    job_titles = ["data", "engineer", "software", "machine"]
    locations = ["berlin--berlin", "cologne", "hamburg--hamburg", "munich"]

    # For each job title and location combination, scrape the job listings
    for title in job_titles:
        for loc in locations:
            scraper = WhatjobsDataExtractor(title, loc)
            scraper.scrape_all_pages()
