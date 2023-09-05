# Import necessary libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from datetime import datetime
from pymongo import MongoClient
import os

# Define constants
BASE_URL = 'https://www.linkedin.com'
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"
}

class JobSearchLinkedIn:
    def __init__(self, keyword, location, page = 1):
        self.search_keyword = keyword
        self.search_location = location
        self.base_url = f'https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={keyword}&location={location}&currentJobId=3638465660&start={page}'
        self.file_number = self.file_number
        self.job_number = 1
        self.filtered_jobs_buffer = []

        # Ensure the directory exists
        self.directory_path = "json_files/linkedin_json_files"
        if not os.path.exists(self.directory_path):
            os.makedirs(self.directory_path)

    def scrape_jobs(self):
        # Get job IDs for the given keyword
        job_ids = self.get_job_ids(self.search_keyword, self.search_location)

        search = {
            "keyword": self.search_keyword,
            "location": self.search_location
        }

        # Get job details for each job ID
        job_details = [self.get_job_details(job_id, search) for job_id in job_ids]

        # Create a DataFrame from the job details
        df = pd.DataFrame(job_details)


        # Save the DataFrame to a JSON/CSV file with a unique name
        self.save_jobs(df, "json")

    # Function to get job IDs from LinkedIn for a given keyword
    def get_job_ids(self, keyword, location):
        job_ids = []
        for page in range(0, 5):
            # Construct the URL for the job search using the keyword and location
            # target_url = f'https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={keyword}&location=Berlin%2C%20Germany&geoId=103035651&currentJobId=3638465660&start={page}'
            # target_url = f'https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={keyword}&location={location}&geoId=103035651&currentJobId=3638465660&start={page}'
            target_url = f'https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={keyword}&location={location}&currentJobId=3638465660&start={page}'
            # Send a GET request to the URL with custom headers
            res = requests.get(target_url, headers=HEADERS)
            # Parse the response text using BeautifulSoup
            soup = BeautifulSoup(res.text, 'html.parser')
            # Find all job listings on the page
            all_jobs_on_this_page = soup.find_all("li")
            # Extract job IDs from each job listing and append to the job_ids list
            job_ids.extend(job.find("div", {"class": "base-card"}).get('data-entity-urn').split(
                ":")[3] for job in all_jobs_on_this_page if job.find("div", {"class": "base-card"}))
        # return job_ids
        return job_ids[:3]

    # Function to get job details for a given job ID


    def get_job_details(self, job_id, search):
        print(search)
        target_url = f'https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}'
        # Send a GET request to the URL with custom headers
        resp = requests.get(target_url, headers=HEADERS)
        # Parse the response text using BeautifulSoup
        soup = BeautifulSoup(resp.text, 'html.parser')

        # Extract relevant job details from the page
        company = soup.find("div", {"class": "top-card-layout__card"})
        company = company.find("a").find("img").get('alt') if company else None

        job_title = soup.find("div", {"class": "top-card-layout__entity-info"})
        job_title = job_title.find("a").text.strip() if job_title else None

        level = soup.find("ul", {"class": "description__job-criteria-list"})
        level = level.find("li").text.replace(
            "Seniority level", "").strip() if level else None

        job_linkedin_id = soup.find(
            "code", {"id": "decoratedJobPostingId"}).string.strip().replace('<!--', '').replace('-->', '')

        job_linkedin_url = target_url

        print(job_linkedin_url)

        amount_applicants_text = soup.select_one(
            ".num-applicants__caption").text.strip()
        amount_applicants = re.compile(r'\d+').findall(amount_applicants_text)

        job_criteria_items = soup.find_all(
            "li", {"class": "description__job-criteria-item"})
        seniority_level_text = job_criteria_items[0].select_one(
            ".description__job-criteria-text").text.strip()
        employment_type_text = job_criteria_items[1].select_one(
            ".description__job-criteria-text").text.strip()
        job_function_text = job_criteria_items[2].select_one(
            ".description__job-criteria-text").text.strip()
        industries_text = job_criteria_items[3].select_one(
            ".description__job-criteria-text").text.strip()

        description = soup.select_one(".description__text").select_one(
            ".show-more-less-html__markup")
        description_contents = description.text.strip()
        # description_contents = description.contents
        # print("contents")
        # print(description.contents)
        # print("string")
        # print(description.string)

        company2_html = soup.select_one(
            ".topcard__org-name-link")
        company2_name = company2_html.string.strip() if company2_html else None

        company_linkedin_url = company2_html['href']
        company_linkedin_url = company_linkedin_url.split('?')[0]
        # print(company_linkedin_url)

        job_location_html = soup.select_one(
            ".topcard__flavor-row").select_one(".topcard__flavor--bullet")
        job_location = job_location_html.text.strip() if job_location_html else None
        # print(job_location)

        return {
            "company_name": company,
            # "company2_name": company2_name,
            "company_linkedin_url": company_linkedin_url,
            "job_title": job_title,
            "job_level": level,
            "job_location": job_location,
            "job_linkedin_id": job_linkedin_id,
            "job_linkedin_url": job_linkedin_url,
            "job_amount_applicants": amount_applicants[0],
            "job_seniority_level_text": seniority_level_text,
            "job_employment_type_text": employment_type_text,
            "job_job_function_text": job_function_text,
            "job_industries_text": industries_text,
            # "job_description": description_contents.encode('utf-8', 'strict'),
            # "job_description": [content.prettify() for content in description_contents],
            "job_description": description_contents,
            "search_datetime": datetime.now().isoformat(),
            "search_keyword": search["keyword"],
            "search_location": search["location"]
        }

    def save_jobs(self, df, type = "json"):

        file_number = self.file_number
        while True:
            file_name = f'{self.directory_path}/linkedin_jobs_{file_number}.{type}'
            try:
                if (type == "csv"):
                    df.to_csv(file_name, index=False, encoding='utf-8')
                elif (type == "json"):
                    df.to_json(file_name, orient="records", indent=4)

                print(
                    f"The search has finished, and the file '{file_name}' has been created.")
                break
            except FileExistsError:
                file_number += 1


def main(self):
    # Get user input for the keyword
    # keyword = input("Enter the keyword for the job search: ")
    keyword = "Data Engineering"
    location = "Berlin, Germany"

    # Get job IDs for the given keyword
    job_ids = self.get_job_ids(keyword, location)

    # print("job_ids:")
    # print(job_ids)

    search = {
        "keyword": keyword,
        "location": location
    }

    # print(search)

    # Get job details for each job ID
    job_details = [self.get_job_details(job_id, search) for job_id in job_ids]

    # print("job_details:")
    # print(job_details)

    # Create a DataFrame from the job details
    df = pd.DataFrame(job_details)

    # Eliminate all blank rows from the DataFrame
    # df.dropna(inplace=True)

    # Print the DataFrame
    print("Job Details:")
    print(df)

    # Save the DataFrame to a JSON/CSV file with a unique name
    self.save_jobs(df, "json")

    # mongoDB
    client = MongoClient(host="127.0.0.1", port = 27017)
    db = client['job_search']
    collection = db['linkedin']

    # Save job to Database
    for job in job_details:
        collection.insert_one(job)


if __name__ == "__main__":
    # main()
    scraper = JobSearchLinkedIn("Data Engineering", "Berlin, Germany")
    scraper.scrape_jobs()
