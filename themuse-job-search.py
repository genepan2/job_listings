import requests  # For making HTTP requests to APIs and websites
import json      # For working with JSON data
import re        # For working with regular expressions
import unicodedata  # For handling Unicode characters and normalization


# Function to make a GET request and handle exceptions
def get_requests_result(url, page_number):
    try:
        res = requests.get(url + str(page_number))
        res.raise_for_status()
        return res
    except requests.exceptions.RequestException as e:
        raise Exception(f"Requesting page {page_number} failed: {e}")


# Function to extract JSON content from response
def get_json_content(res):
    return res.json()['results']


# Function to strip HTML tags from text
def strip_html_tags(text):
    clean_text = re.sub(r'<.*?>', '', text)
    return clean_text


# Function to translate Unicode characters to ASCII
def translate_unicode_characters(text):
    translated_text = ''.join(
        char if ord(char) < 128 else unicodedata.normalize('NFKD', char).encode('ascii', 'ignore').decode('utf-8')
        for char in text
    )
    return translated_text


# Function to remove specified fields from JSON object
def remove_some_tags(obj):
    if isinstance(obj, dict):
        obj.pop('short_name', None)
        obj.pop('id', None)
        for key, value in obj.items():
            remove_some_tags(value)
    elif isinstance(obj, list):
        for item in obj:
            remove_some_tags(item)


# Display category options and get user input
print("Select a job category:")
print("1 - Data and Analytics")
print("2 - Software Engineering")
category_choice = input("Enter the number corresponding to your desired job category: ")

# Determine the category based on user input
if category_choice == "1":
    category = "Data%20and%20Analytics"
elif category_choice == "2":
    category = "Software%20Engineering"
else:
    print("Invalid choice. Please select a valid option.")
    exit()

# Construct the API URL with the selected category and location
url_themuse = f"https://www.themuse.com/api/public/jobs?category={category}&location=Berlin%2C%20Germany&page="

# Initialize an empty list to store filtered jobs
filtered_jobs = []

# Loop through a range of pages to retrieve job listings
for page_number in range(0, 5):
    try:
        # Make a GET request to retrieve job listings
        res = get_requests_result(url_themuse, page_number)
        json_content = get_json_content(res)

        # Process job listings, removing specified fields
        for job in json_content:
            remove_some_tags(job)

            job['contents'] = strip_html_tags(job['contents'])
            job['contents'] = translate_unicode_characters(job['contents'])

            # Remove specified fields
            del job['tags']
            del job['model_type']
            del job['type']

            # Filter jobs for location "Berlin, Germany"
            if any(location['name'] == 'Berlin, Germany' for location in job['locations']):
                filtered_jobs.append(job)
    except Exception as exception:
        print(exception)
        break

# Save filtered jobs in JSON format
json_filename = "filtered_jobs.json"
with open(json_filename, 'w') as json_file:
    json.dump(filtered_jobs, json_file, indent=4)  # Indent for better readability

# Save filtered jobs in NDJSON format
ndjson_filename = "filtered_jobs.ndjson"
with open(ndjson_filename, 'w') as ndjson_file:
    for job in filtered_jobs:
        json.dump(job, ndjson_file)
        ndjson_file.write('\n')

print(f"Filtered jobs saved in '{json_filename}' in JSON format.")
print(f"Filtered jobs saved in '{ndjson_filename}' in NDJSON format.")

