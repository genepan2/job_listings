# Import necessary libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re

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
        job_ids.extend(job.find("div", {"class": "base-card"}).get('data-entity-urn').split(
            ":")[3] for job in all_jobs_on_this_page if job.find("div", {"class": "base-card"}))
    # return job_ids
    return job_ids[:3]

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
    description_contents = description.contents
    # print("contents")
    # print(description.contents)
    # print("string")
    # print(description.string)

    return {
        "company": company,
        "job-title": job_title,
        "level": level,
        "job_linkedin_id": job_linkedin_id,
        "job_linkedin_url": job_linkedin_url,
        "amount_applicants": amount_applicants[0],
        "seniority_level_text": seniority_level_text,
        "employment_type_text": employment_type_text,
        "job_function_text": job_function_text,
        "industries_text": industries_text,
        "description": description_contents
    }


def main():
    # Get user input for the keyword
    # keyword = input("Enter the keyword for the job search: ")
    keyword = "Data Engineering"

    # Get job IDs for the given keyword
    job_ids = get_job_ids(keyword)

    # Get job details for each job ID
    job_details = [get_job_details(job_id) for job_id in job_ids]

    # Create a DataFrame from the job details
    df = pd.DataFrame(job_details)

    # Eliminate all blank rows from the DataFrame
    df.dropna(inplace=True)

    # Print the DataFrame
    print("Job Details:")
    print(df)

    # Save the DataFrame to a CSV file with a unique name
    file_number = 1
    while True:
        file_name = f'jobs{file_number}.csv'
        try:
            df.to_csv(file_name, index=False, encoding='utf-8')
            print(
                f"The search has finished, and the file '{file_name}' has been created.")
            break
        except FileExistsError:
            file_number += 1


if __name__ == "__main__":
    main()
