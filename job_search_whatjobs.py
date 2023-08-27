import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime
import unicodedata
import os

# Parameters for job title and location
job_title = "data-engineer"
location = "berlin--berlin"

base_url = f"https://de.whatjobs.com/jobs/{job_title}/{location}"

# Ensure the directory exists
directory_path = "json_files"
if not os.path.exists(directory_path):
    os.makedirs(directory_path)

output_filename = f"{directory_path}/whatjobs_jobs_{{}}.json"
num_pages_to_scrape = 3
global_job_number = 0

def get_full_description(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    description_div = soup.find("div", class_="dDesc")
    if description_div:
        description = description_div.get_text(separator=' ', strip=True)
        description = description.replace("\n", " ")

        # Normalize the description text
        description = unicodedata.normalize("NFC", description)
        return description

    return "N/A"

def scrape_page(url):
    global global_job_number

    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    job_listings = soup.find_all("div", class_="searchResultItem")

    jobs_data = []

    for job in job_listings:
        title_element = job.find("h2", class_="title")
        title = title_element.text.strip() if title_element else "N/A"
        
        location_element = job.find("div", class_="posR")
        location = location_element.text.split(' ', 1)[1].strip() if location_element else "N/A"
        
        company_element = job.find("span", class_="wjIcon24 companyName")
        company = company_element.find_parent('div').text.strip() if company_element else "N/A"
        
        date_published_element = job.find("span", class_="wjIcon24 jobAge")
        date_published = date_published_element.find_parent('div').text.strip() if date_published_element else "N/A"
        
        description_element = job.find("span", class_="jDesc")
        description = description_element.text.strip() if description_element else "N/A"
        job_url = description_element.find_next('a')['href'] if description_element else "N/A"

        full_description = get_full_description(job_url)
        
        job_data = {
            "job_number": global_job_number,
            "job_title": title,
            "job_location": location,
            "company_name": company,
            "publication_date": date_published,
            "job_description": full_description,
            "job_url": job_url,
            "search_datetime": datetime.now().isoformat(),
            "search_location": location
        }

        jobs_data.append(job_data)
        global_job_number += 1

    return jobs_data

def scrape_all_pages(base_url, num_pages):
    all_jobs = []
    file_count = 1

    for page in range(1, num_pages + 1):
        page_url = f"{base_url}?page={page}"
        jobs_on_page = scrape_page(page_url)
        
        if not jobs_on_page:
            break
        
        all_jobs.extend(jobs_on_page)

        # Split the results into separate files containing 20 jobs each
        if len(all_jobs) >= 20:
            with open(output_filename.format(file_count), "w") as outfile:
                json.dump(all_jobs[:20], outfile, ensure_ascii=False, indent=4)
            all_jobs = all_jobs[20:]
            file_count += 1

    # Write any remaining jobs to file
    if all_jobs:
        with open(output_filename.format(file_count), "w") as outfile:
            json.dump(all_jobs, outfile, ensure_ascii=False, indent=4)

    print(f"Scraped jobs and saved to files.")

if __name__ == "__main__":
    scrape_all_pages(base_url, num_pages_to_scrape)
