import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch
from app.api_jwt import api, DbQuery, JobRequest, JobLevel, JobLocation, UserSchema

# Instantiate the TestClient with the FastAPI application instance
client = TestClient(api)

# Sample data to be returned when the DbQuery.query_jobs method is called
sample_data = [
    {"job_title": "Software Engineer", "company": "ABC Corp", "location": "Berlin"},
    {"job_title": "Data Scientist", "company": "XYZ Ltd", "location": "Munich"},
]

# Test case for the GET /jobs endpoint
def test_get_jobs():
    with patch.object(DbQuery, 'query_jobs', return_value=sample_data):
        response = client.get(
            "/jobs",
            params={
                "level": "entry",
                "age": 1,
                "order": "asc",
                "page": 1,
                "items_per_page": 10,
                "location": ["Berlin", "Munich"]
            }
        )
        
    assert response.status_code == 200
    assert response.json()['data'] == sample_data

# Test case for the POST /jobs endpoint
def test_post_jobs():
    # First, let's create a user and get the JWT token
    user = UserSchema(username="testuser", password="testpass")
    response = client.post("/user/signup", json=user.model_dump())
    assert response.status_code == 200
    token = response.json()['access_token']

    headers = {"Authorization": f"Bearer {token}"}

    with patch.object(DbQuery, 'query_jobs', return_value=sample_data):
        job_request = JobRequest(
            level=[JobLevel.entry],
            location=[JobLocation.berlin, JobLocation.munich],
            age=1,
            order='asc',
            page=1,
            items_per_page=10
        )
        response = client.post("/jobs", json=job_request.model_dump(), headers=headers)
    
    assert response.status_code == 200
    assert response.json()['data'] == sample_data