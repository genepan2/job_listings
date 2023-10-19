import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch
from api import api, DbQuery, JobRequest, JobLevel, JobLocation

# Instantiate the TestClient with the FastAPI application instance
client = TestClient(api)

# Sample data to be returned when the DbQuery.query_jobs method is called
# We use sample data because unit test is meant to test only the code in the function you are testing
sample_data = [
    {"job_title": "Software Engineer", "company": "ABC Corp", "location": "Berlin"},
    {"job_title": "Data Scientist", "company": "XYZ Ltd", "location": "Munich"},
]

# Test case for the GET /jobs endpoint
def test_get_jobs():
    """
    Test if the GET /jobs endpoint returns the expected data and status code.
    The DbQuery.query_jobs method is patched to return sample data.
    """
    # Patch the query_jobs method of the DbQuery class to return sample data
    with patch.object(DbQuery, 'query_jobs', return_value=sample_data):
        # Make a request to the GET /jobs endpoint with given parameters
        response = client.get(
            "/jobs",
            params={
                "keyword": "engineer",
                "level": "entry",
                "age": 1,
                "order": "asc",
                "page": 1,
                "items_per_page": 10,
                "location": ["Berlin", "Munich"]
            }
        )

    # Assert that the response status code is 200 (OK)
    assert response.status_code == 200
    # Assert that the data returned in the response matches the sample data
    assert response.json()['data'] == sample_data

# Test case for the POST /jobs endpoint
def test_post_jobs():
    """
    Test if the POST /jobs endpoint returns the expected data and status code.
    The DbQuery.query_jobs method is patched to return sample data.
    """
    # Patch the query_jobs method of the DbQuery class to return sample data
    with patch.object(DbQuery, 'query_jobs', return_value=sample_data):
        # Create a JobRequest instance with given parameters
        job_request = JobRequest(
            level=[JobLevel.entry],
            location=[JobLocation.berlin, JobLocation.munich],
            age=1,
            order='asc',
            page=1,
            items_per_page=10
        )
        # Make a request to the POST /jobs endpoint with the JobRequest instance
        response = client.post("/jobs", json=job_request.model_dump())

    # Assert that the response status code is 200 (OK)
    assert response.status_code == 200
    # Assert that the data returned in the response matches the sample data
    assert response.json()['data'] == sample_data
