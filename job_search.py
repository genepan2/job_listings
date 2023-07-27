# Import necessary libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd

# Define constants
BASE_URL = 'https://www.linkedin.com'
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"
}

# Function to get job IDs from LinkedIn for a given keyword
def get_job_ids(keyword):
    job_ids = []
    for page in range(0, 5):
        # Construct the URL for the job search using the keyword and location
        target_url = f'https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={keyword}&location=Berlin%2C%20Germany&geoId=103035651&currentJobId=3638465660&start={page}'
        # Send a GET request to the URL with custom headers
        res = requests.get(target_url, headers=HEADERS)
        # Parse the response text using BeautifulSoup
        soup = BeautifulSoup(res.text, 'html.parser')
        # Find all job listings on the page
        all_jobs_on_this_page = soup.find_all("li")
        # Extract job IDs from each job listing and append to the job_ids list
        job_ids.extend(job.find("div", {"class": "base-card"}).get('data-entity-urn').split(":")[3] for job in all_jobs_on_this_page if job.find("div", {"class": "base-card"}))
    return job_ids

# Function to get job details for a given job ID
def get_job_details(job_id):
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
    level = level.find("li").text.replace("Seniority level", "").strip() if level else None

    return {"company": company, "job-title": job_title, "level": level}

# Main function
def main():
    # Get user input for the keyword
    keyword = input("Enter the keyword for the job search: ")

    # Get job IDs for the given keyword
    job_ids = get_job_ids(keyword)
    
    # Get job details for each job ID
    job_details = [get_job_details(job_id) for job_id in job_ids]

    # Create a DataFrame from the job details and save it to a CSV file with a unique name
    # Here, the file will be saved with a name like 'jobs1.csv', 'jobs2.csv', etc., incrementing the number for each new file
    df = pd.DataFrame(job_details)
    file_number = 1
    while True:
        file_name = f'jobs{file_number}.csv'
        try:
            df.to_csv(file_name, index=False, encoding='utf-8')
            break
        except FileExistsError:
            file_number += 1

if __name__ == "__main__":
    main()
