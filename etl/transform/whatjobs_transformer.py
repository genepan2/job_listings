import json
import os
import requests
from bs4 import BeautifulSoup
import unicodedata

class WhatjobsDataTransformer:
    """
    This class is responsible for transforming raw job data scraped from WhatJobs. 
    Transformation includes fetching full job descriptions and normalizing data.
    """

    def __init__(self):
        """Initialize transformer with input and output directories."""
        self.input_directory = "data/raw/whatjobs_json_files"
        self.output_filename = "data/processed/whatjobs_json_files/whatjobs_cleaned_data.json"

    def get_full_description(self, url):
        """
        Fetch the full job description from a given URL.

        Args:
        - url (str): The URL from which to fetch the job description.

        Returns:
        - str: The fetched job description, or "N/A" if an error occurs.
        """
        # Check if the URL is valid
        if not url.startswith("http"):
            print(f"Skipping invalid URL: {url}")
            return "N/A"

        # Attempt to fetch the job description from the provided URL
        try:
            response = requests.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            description_div = soup.find("div", class_="dDesc")
            if description_div:
                description = description_div.get_text(separator=' ', strip=True)
                description = description.replace("\n", " ")
                description = unicodedata.normalize("NFC", description)
                return description
        except Exception as e:
            print(f"Error fetching description for URL {url}. Error: {e}")
            return "N/A"

    def transform_data(self):
        """Transform raw job data by fetching full descriptions and normalizing fields."""
        print("Starting transformation...")
        all_jobs = []
        counter = 0

        for filename in os.listdir(self.input_directory):
            if filename.startswith("whatjobs_raw_data.json"):
                with open(os.path.join(self.input_directory, filename), "r") as infile:
                    jobs = json.load(infile)
                    
                    for job in jobs:
                        try:
                            job["job_description"] = self.get_full_description(job["job_url"])
                            counter += 1
                            # Printing info for every 50 transformed elements
                            if counter % 50 == 0:
                                print(f"Transformed {counter} jobs...")
                        except Exception as e:
                            continue

                    all_jobs.extend(jobs)

        with open(self.output_filename, "w") as outfile:
            json.dump(all_jobs, outfile, ensure_ascii=False, indent=4)

        print(f"Transformation finished. {len(all_jobs)} jobs saved in '{self.output_filename}'.")

def main():
    """Main function to initiate the data transformation process."""
    transformer = WhatjobsDataTransformer()
    transformer.transform_data()

if __name__ == "__main__":
    main()
