from unittest.mock import patch
from bs4 import BeautifulSoup
from whatjobs_extractor import WhatjobsDataExtractor


# This is a sample HTML snippet that mimics the structure of the actual web page
SAMPLE_HTML_CONTENT = '''
<div class="searchResultItem">
    <h2 class="title">Data Scientist</h2>
    <div class="posR">Location: Berlin</div>
    <div><span class="wjIcon24 companyName">The Company</span></div>
    <div><span class="wjIcon24 jobAge">Yesterday</span></div>
    <span class="jDesc">
        <a href="https://example.com/job-listing">Job Listing</a>
    </span>
</div>
'''

# This is another sample HTML snippet for the detailed job description page
SAMPLE_JOB_DESCRIPTION_HTML = '''
<div class="dDesc">
    This is a detailed job description.
</div>
'''

@patch('requests.get')
def test_scrape_page(mock_get, tmp_path):
    # Configure the mock to return a response with the sample HTML content
    mock_get.return_value.content = SAMPLE_HTML_CONTENT.encode()

    # Create an instance of the WhatjobsDataExtractor
    extractor = WhatjobsDataExtractor("Data Scientist", "Berlin")

    # Call the scrape_page method and get the jobs data
    jobs_data = extractor.scrape_page("https://fakeurl.com")

    # Assert that one job was extracted
    assert len(jobs_data) == 1  

    # Check the extracted job data
    job = jobs_data[0]
    assert job['title'] == 'Data Scientist'
    assert job['location'] == 'Berlin'
    assert job['company_name'] == 'The Company'

@patch('requests.get')
def test_get_full_description(mock_get):
    # Configure the mock to return a response with the sample job description HTML
    mock_get.return_value.content = SAMPLE_JOB_DESCRIPTION_HTML.encode()

    # Create an instance of the WhatjobsDataExtractor
    extractor = WhatjobsDataExtractor("Data Scientist", "Berlin")

    # Get the full job description
    description = extractor.get_full_description("https://fakeurl.com/job-listing")

    # Check the extracted job description
    assert description == "This is a detailed job description."

# Add more tests as needed to cover other parts of the WhatjobsDataExtractor class
