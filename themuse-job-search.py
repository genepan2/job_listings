import requests  # Import the 'requests' module to make HTTP requests to APIs and websites
import json  # Import the 'json' module to work with JSON data, such as parsing and serializing
import re  # Import the 're' module for working with regular expressions, useful for text manipulation
import unicodedata  # Import the 'unicodedata' module for working with Unicode characters and normalization

# Function to make a GET request and handle exceptions
def get_requests_result(url, page_number):
    """
    Make a GET request to the specified URL and handle exceptions.

    Parameters:
    url : str
        The URL to make the GET request to.
    page_number : int
        The page number to append to the URL.

    Returns:
    requests.Response
        The response object from the HTTP request.
    Raises:
    Exception
        If the request or response encounter an error.
    """
    try:
        res = requests.get(url + str(page_number))
        res.raise_for_status()
        return res
    except requests.exceptions.RequestException as e:
        raise Exception(f"Requesting page {page_number} failed: {e}")

# Function to extract JSON content from response
def get_json_content(res):
    """
    Extract JSON content from the response object.

    Parameters:
    res : requests.Response
        The response object from the HTTP request.

    Returns:
    dict
        The extracted JSON content.
    """
    return res.json()['results']

# Function to strip HTML tags from text
def strip_html_tags(text):
    """
    Remove HTML tags from the given text using regular expressions.

    Parameters:
    text : str
        The input text containing HTML tags.

    Returns:
    str
        The input text with HTML tags removed.
    """
    clean_text = re.sub(r'<.*?>', '', text)
    return clean_text

# Function to translate Unicode characters to ASCII
def translate_unicode_characters(text):
    """
    Translate Unicode characters to ASCII or remove them.

    Parameters:
    text : str
        The input text containing Unicode characters.

    Returns:
    str
        The input text with Unicode characters translated to ASCII.
    """
    translated_text = ''.join(
        char if ord(char) < 128 else unicodedata.normalize('NFKD', char).encode('ascii', 'ignore').decode('utf-8')
        for char in text
    )
    return translated_text


# Function to remove specified fields from JSON object
def remove_some_tags(obj):
    """
    Recursively remove specified fields from a JSON-like object.

    This function removes the specified fields 'short_name', 'id', 'model_type', 'tags', and 'type'
    from a JSON-like object. It operates recursively, traversing through dictionaries and lists.

    Parameters:
    obj : dict or list
        The JSON-like object to remove fields from.

    Returns:
    None
    """
    if isinstance(obj, dict):
        obj.pop('short_name', None)
        obj.pop('id', None)
        obj.pop('model_type', None)
        obj.pop('tags', None)
        obj.pop('type', None)
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
