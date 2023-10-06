import json
import re
import unicodedata
from datetime import datetime
import os
from config.constants import FIELDS
from langdetect import detect

class ThemuseDataTransformer:
    """
    A class to handle the transformation of job data extracted from TheMuse API.
    This includes removing HTML tags, translating Unicode characters, and reformatting the data.

    Attributes:
        directory_path (str): Directory where the raw data is located.
        processed_directory_path (str): Directory where the transformed data will be saved.
        job_number (int): A counter used for numbering the transformed jobs.
    """

    def __init__(self):
        """Initialize the ThemuseDataTransformer with predefined settings."""
        self.directory_path = "data/raw/themuse_json_files"
        self.processed_directory_path = "data/processed/themuse_json_files"
        self.job_number = 1

        # Ensure the processed directory exists
        if not os.path.exists(self.processed_directory_path):
            os.makedirs(self.processed_directory_path)

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

    def detect_language(self, text):
        """Detect the language of the text.

        Parameters:
            text (str): The text whose language is to be detected.

        Returns:
            str: The language of the text.
        """
        try:
            lang = detect(text)
            if lang == 'en':
                return 'English'
            elif lang == 'de':
                return 'German'
            else:
                return 'Other'
        except Exception as e:
            print(f"Error detecting language. Error: {e}")
            return 'Unknown'
    
    def transform_job_listing(self, job):
        """
        Transform a single job listing's data format.

        Parameters:
            job (dict): Raw job data.

        Returns:
            dict: Transformed job data.
        """
        job_title = job.get("name", "").lower()
        job_level = "Middle"

        if "senior" in job_title:
            job_level = "Senior"
        elif "junior" in job_title:
            job_level = "Junior"
        elif "intern" in job_title or "internship" in job_title:
            job_level = "Intern"
        elif "student" in job_title or "working student" in job_title:
            job_level = "Student"
        elif "lead" in job_title:
            job_level = "Lead"
        elif "head" in job_title:
            job_level = "Head"

        job_description = self.strip_html_tags(job.get("contents", ""))
        language = self.detect_language(job_description)

        transformed_job = {
            FIELDS["number"]: f"themuse-{self.job_number}",
            FIELDS["title"]: job.get("name", ""),
            FIELDS["company_name"]: job.get("company", {}).get("name", ""),
            FIELDS["location"]: job.get("locations", [{}])[0].get("name", "") if job.get("locations") else "",
            FIELDS["search_keyword"]: job.get("categories", [{}])[0].get("name", "") if job.get("categories") else "",
            FIELDS["level"]: job_level,  
            FIELDS["publish_date"]: job.get("publication_date", ""),
            FIELDS["url"]: job.get("refs", {}).get("landing_page", ""),
            FIELDS["search_datetime"]: datetime.now().isoformat(),
            FIELDS["description"]: job_description,
            FIELDS["language"]: language  
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
            if "Germany" in job_location:
                job["contents"] = self.translate_unicode_characters(job.get("contents", ""))
                transformed_job = self.transform_job_listing(job)
                transformed_jobs.append(transformed_job)
                self.job_number += 1

        with open(f"{self.processed_directory_path}/themuse_cleaned_data.json", 'w') as json_file:
            json.dump(transformed_jobs, json_file, indent=4)

# Main execution
if __name__ == "__main__":
    transformer = ThemuseDataTransformer()
    transformer.transform_jobs()
