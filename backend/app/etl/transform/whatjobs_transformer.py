import json
import os
from langdetect import detect

class WhatjobsDataTransformer:
    """
    This class is responsible for transforming raw job data scraped from WhatJobs. 
    Transformation includes normalizing data and detecting the language of the job descriptions.
    """

    def __init__(self):
        """Initialize transformer with input and output directories."""
        self.input_directory = 'data/raw/whatjobs_json_files'
        self.output_filename = "data/processed/whatjobs_json_files/whatjobs_cleaned_data.json"

    def detect_language(self, text):
        """
        Detect the language of the given text.

        Args:
        - text (str): The text whose language is to be detected.

        Returns:
        - str: The detected language, or "Unknown" if an error occurs.
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
            return 'Other'
    
    def transform_data(self):
        """
        Transform the raw job data by detecting the language of the job descriptions
        and normalizing the job levels based on keywords in the job titles.
        """
        print("Starting transformation...")
        all_jobs = []
        counter = 0

        for filename in os.listdir(self.input_directory):
            if filename.endswith(".json"):
                with open(os.path.join(self.input_directory, filename), "r") as infile:
                    jobs = json.load(infile)
                    
                    for job in jobs:
                        try:
                            job_title = job.get("title", "").lower()
                            if "senior" in job_title:
                                job["level"] = "Senior"
                            elif "junior" in job_title:
                                job["level"] = "Junior"
                            elif "intern" in job_title or "internship" in job_title:
                                job["level"] = "Intern"
                            elif "lead" in job_title:
                                job["level"]= "Lead"
                            elif "head" in job_title:
                                job["level"] = "Head"
                            elif "student" in job_title or "working student" in job_title:
                                job["level"] = "Student"
                            else:
                                job["level"] = "Middle"

                            job["language"] = self.detect_language(job["description"])

                            counter += 1
                            if counter % 50 == 0:
                                print(f"Transformed {counter} jobs...")
                        except Exception as e:
                            print(f"Error in job transformation. Error: {e}")
                            continue

                    all_jobs.extend(jobs)

        with open(self.output_filename, "w") as outfile:
            json.dump(all_jobs, outfile, ensure_ascii=False, indent=4)

        print(f"Transformation finished. {len(all_jobs)} jobs saved in '{self.output_filename}'.")

def main():
    """
    Main function to execute the job data transformation.
    """
    transformer = WhatjobsDataTransformer()
    transformer.transform_data()

if __name__ == "__main__":
    main()
