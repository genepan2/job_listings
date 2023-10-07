import json
import os
import requests
from bs4 import BeautifulSoup
import unicodedata
from datetime import datetime
from config.constants import FIELDS

NUM_PAGES_TO_SCRAPE = 1  # The number of pages to be scraped from each search result

class WhatjobsDataExtractor:
    """
    Class responsible for extracting job listing data from WhatJobs based on provided job title and location.
    """
    global_job_number = 1   # Class level attribute to ensure each job gets a unique identifier
    all_jobs = []  # Class level attribute to store all jobs across different job titles and locations

    def __init__(self, job_title, location, items=None):
        """
        Initialize the extractor with job title and location.

        Parameters:
        - job_title (str): The job title to search for.
        - location (str): The location to search within.
        - items (int, optional): The number of items to scrape. Defaults to None, meaning all items will be scraped.
        """
        self.job_title = job_title
        self.location = location
        self.items = items  # The number of items to scrape on each page
        self.base_url = f"https://de.whatjobs.com/jobs/{self.job_title}/{self.location}"  # The base URL for the search query
        self.num_pages_to_scrape = NUM_PAGES_TO_SCRAPE  # The number of pages to scrape for each search query
        self.output_filename = "data/raw/whatjobs_json_files/whatjobs_raw_data.json"  # The file where the scraped data will be stored

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
        # Sending a request to the URL, and getting the content
        response = requests.get(url)
        # Using BeautifulSoup to parse the HTML content
        soup = BeautifulSoup(response.content, 'html.parser')
        # Finding all job listings on the page
        job_listings = soup.find_all("div", class_="searchResultItem")
        # If the number of items to scrape is specified, slice the list to get the required number of items
        job_listings = job_listings[:self.items] if self.items else job_listings
        jobs_data = []  # A list to store the job data dictionaries

        # Loop through all job listings to extract the job data
        for job in job_listings:
            # Extracting various data elements from the job listing
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

            # Collating the extracted data into a dictionary
            job_data = {
                FIELDS["number"]: f"whatjobs-{WhatjobsDataExtractor.global_job_number}",
                FIELDS["company_name"]: company,
                FIELDS["title"]: title,
                FIELDS["location"]: location,
                FIELDS["publish_date"]: date_published,
                FIELDS["description"]: full_description,  # Updated to include the full description
                FIELDS["url"]: job_url,
                FIELDS["search_datetime"]: datetime.now().isoformat(),
                FIELDS["search_keyword"]: self.job_title,
                FIELDS["search_location"]: location
            }

            # Adding the job data dictionary to the jobs_data list
            jobs_data.append(job_data)
            WhatjobsDataExtractor.global_job_number += 1  # Incrementing the global job number

        return jobs_data  # Returning the list of job data dictionaries

    def scrape_all_pages(self):
        """
        Scrape job data from all pages of WhatJobs based on the initialized job title and location.
        """
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
        print(f"Saved all {len(WhatjobsDataExtractor.all_jobs)} jobs in '{self.output_filename}'.")

    def get_full_description(self, url):
        """
        Fetch the full job description from a given URL.

        Parameters:
        - url (str): The URL of the job listing.

        Returns:
        - str: The full job description.
        """
        # Sending a request to the job URL
        response = requests.get(url)
        # Using BeautifulSoup to parse the HTML content
        soup = BeautifulSoup(response.content, 'html.parser')
        # Finding the job description div
        description_div = soup.find("div", class_="dDesc")

        # If the description div is found, extract and return the text content
        if description_div:
            description = description_div.get_text(separator=' ', strip=True)
            description = description.replace("\n", " ")
            description = unicodedata.normalize("NFC", description)
            return description
        else:
            print(f"Description not found for URL: {url}")
            return "N/A"  # Return "N/A" if the description is not found

if __name__ == "__main__":
    # List of job titles and locations to search for
    job_titles = ["data", "engineer", "software", "machine"]
    locations = ["berlin--berlin", "cologne", "hamburg--hamburg", "munich"]

    # For each job title and location combination, scrape the job listings
    for title in job_titles:
        for loc in locations:
            scraper = WhatjobsDataExtractor(title, loc)
            scraper.scrape_all_pages()
