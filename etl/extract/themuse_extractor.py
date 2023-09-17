# DataExtractor.py

import requests
import json
import os

class ThemuseDataExtractor:
    """
    A class responsible for extracting job data from TheMuse's API for specific categories and locations.
    
    Attributes:
        base_url (str): The base API URL with pre-set parameters.
        directory_path (str): Directory path where the extracted data will be saved.
    """

    def __init__(self):
        """Initialize ThemuseDataExtractor with predefined settings."""
        self.base_url = ("https://www.themuse.com/api/public/jobs?"
                         "category=Computer%20and%20IT&category=Data%20and%20Analytics&category=Data%20Science&category=IT"
                         "&category=Science%20and%20Engineering&category=Software%20Engineer&category=Software%20Engineering"
                         "&location=Berlin%2C%20Germany&location=Cologne%2C%20Germany&location=Hamburg%2C%20Germany&location=Munich%2C%20Germany&page=")
        
        # Ensure the directory exists
        self.directory_path = "data/raw/themuse_json_files"
        if not os.path.exists(self.directory_path):
            os.makedirs(self.directory_path)

    def get_requests_result(self, url, page_number):
        """
        Fetches the result from the API for a given URL and page number.
        
        Parameters:
            url (str): The base API URL.
            page_number (int): Page number for the request.

        Returns:
            Response object if the request is successful.

        Raises:
            Exception: If the API request is unsuccessful.
        """
        try:
            res = requests.get(url + str(page_number))
            res.raise_for_status()
            return res
        except requests.exceptions.RequestException as e:
            raise Exception(f"Requesting page {page_number} failed: {e}")

    def extract_jobs(self):
        """
        Extracts job data from TheMuse's API and saves it to a JSON file.
        Iterates through pages of the API until data is no longer retrieved or an error occurs.
        """
        job_buffer = []
        for page_number in range(0, 20):
            try:
                res = self.get_requests_result(self.base_url, page_number)
                job_buffer.extend(res.json()['results'])
            except Exception as exception:
                print(exception)
                break
        
        # Save the extracted job data to a JSON file
        with open(f"{self.directory_path}/themuse_raw_data.json", 'w') as json_file:
            json.dump(job_buffer, json_file, indent=4)

# Main execution
if __name__ == "__main__":
    extractor = ThemuseDataExtractor()
    extractor.extract_jobs()
