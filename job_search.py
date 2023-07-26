import requests
from bs4 import BeautifulSoup
import pandas as pd

BASE_URL = 'https://www.linkedin.com'
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"
}

def get_job_ids(keyword):
    job_ids = []
    for page in range(0, 10):
        target_url = f'https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={keyword}&location=Berlin%2C%20Germany&geoId=103035651&currentJobId=3638465660&start={page}'
        res = requests.get(target_url, headers=HEADERS)
        soup = BeautifulSoup(res.text, 'html.parser')
        all_jobs_on_this_page = soup.find_all("li")
        job_ids.extend(job.find("div", {"class": "base-card"}).get('data-entity-urn').split(":")[3] for job in all_jobs_on_this_page if job.find("div", {"class": "base-card"}))
    return job_ids

def get_job_details(job_id):
    target_url = f'https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}'
    resp = requests.get(target_url, headers=HEADERS)
    soup = BeautifulSoup(resp.text, 'html.parser')

    company = soup.find("div", {"class": "top-card-layout__card"})
    company = company.find("a").find("img").get('alt') if company else None

    job_title = soup.find("div", {"class": "top-card-layout__entity-info"})
    job_title = job_title.find("a").text.strip() if job_title else None

    level = soup.find("ul", {"class": "description__job-criteria-list"})
    level = level.find("li").text.replace("Seniority level", "").strip() if level else None

    return {"company": company, "job-title": job_title, "level": level}

def main():
    # Get user input for the keyword
    keyword = input("Enter the keyword for the job search: ")

    job_ids = get_job_ids(keyword)
    job_details = [get_job_details(job_id) for job_id in job_ids]

    df = pd.DataFrame(job_details)
    df.to_csv('jobs2.csv', index=False, encoding='utf-8')

if __name__ == "__main__":
    main()
