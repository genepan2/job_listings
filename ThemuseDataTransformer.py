import json
import re
import unicodedata
from datetime import datetime
import os

class ThemuseDataTransformer:
    """
    A class to handle the transformation of job data extracted from TheMuse API.
    This includes removing HTML tags, translating Unicode characters, and reformatting the data.
    
    Attributes:
        directory_path (str): Directory where the raw and transformed data are located and will be saved.
        job_number (int): A counter used for numbering the transformed jobs.
    """
    
    def __init__(self):
        """Initialize the ThemuseDataTransformer with predefined settings."""
        self.directory_path = "json_files/themuse_json_files"
        self.job_number = 1

    def strip_html_tags(self, text):
        """
        Remove HTML tags from a string.

        Parameters:
            text (str): String containing potential HTML tags.

        Returns:
            str: String without HTML tags.
        """
        clean_text = re.sub(r'<.*?>', '', text)
        return clean_text

    def translate_unicode_characters(self, text):
        """
        Translate non-ASCII Unicode characters in a string to their closest ASCII representation.

        Parameters:
            text (str): String with potential Unicode characters.

        Returns:
            str: String with non-ASCII characters translated to ASCII.
        """
        translated_text = ''.join(
            char if ord(char) < 128 else unicodedata.normalize(
                'NFKD', char).encode('ascii', 'ignore').decode('utf-8')
            for char in text
        )
        return translated_text

    def transform_job_listing(self, job):
        """
        Transform a single job listing's data format.

        Parameters:
            job (dict): Raw job data.

        Returns:
            dict: Transformed job data.
        """
        transformed_job = {
            "job_number": f"themuse-{self.job_number}",
            "job_title": job.get("name", ""),
            "company_name": job.get("company", {}).get("name", ""),
            "job_location": job.get("locations", [{}])[0].get("name", "") if job.get("locations") else "",
            "search_keyword": job.get("categories", [{}])[0].get("name", "") if job.get("categories") else "",
            "job_level": job.get("levels", [{}])[0].get("name", "") if job.get("levels") else "",
            "publication_date": job.get("publication_date", ""),
            "job_url": job.get("refs", {}).get("landing_page", ""),
            "search_datetime": datetime.now().isoformat(),
            "job_description": self.strip_html_tags(job.get("contents", ""))
        }
        return transformed_job

    def transform_jobs(self):
        """
        Load raw job data from a JSON file, transform its format, and save the transformed data to another JSON file.
        """
        with open(f"{self.directory_path}/themuse_raw_data.json", 'r') as raw_file:
            raw_jobs = json.load(raw_file)

        transformed_jobs = []
        for job in raw_jobs:
            job_location = job.get("locations", [{}])[0].get("name", "")
            if "Germany" in job_location:  # Check if the word 'Germany' is in job_location
                job["contents"] = self.translate_unicode_characters(job.get("contents", ""))
                transformed_job = self.transform_job_listing(job)
                transformed_jobs.append(transformed_job)
                self.job_number += 1

        # Save the transformed job data to a JSON file
        with open(f"{self.directory_path}/themuse_cleaned_data.json", 'w') as json_file:
            json.dump(transformed_jobs, json_file, indent=4)

# Main execution
if __name__ == "__main__":
    transformer = ThemuseDataTransformer()
    transformer.transform_jobs()
