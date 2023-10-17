# README

## Project Overview

The job_listing project is a comprehensive solution for job listing, retrieval, and management. It consists of a backend application for data processing, an Airflow setup for workflow management, a frontend application for user interaction, and MongoDB for data storage. The project is containerized using Docker for easy deployment and scalability. 

## Technologies

Backend: Python

Frontend: ReactJS

Workflow Management: Airflow

Database: MongoDB

Containerization: Docker

Uni Test: Pytest

Deployment: Git Action, Docker Hub, AWS

## Stages

Data Collection

Data Processing

Data Consumption

Automation

Deployment

## Setup and Installation

Clone the Repository

`git clone https://github.com/leviGab001/job_listing`

`cd job_listing`

Build and Start the Docker Containers

`docker compose --profile frontend --profile backend up`

## Access the Application

Frontend: Open a web browser and navigate to `http://localhost:3000`

Backend API: Send requests to `http://localhost:8000`

## How to Use the API (secure)

You need to start the uvicorn server with the nessaccery private key and certificate:
`python3 -m uvicorn api_jwt:api --reload --ssl-keyfile ./cert/key-no-pass.pem --ssl-certfile ./cert/cert.pem`

Because this certificate is self-signed, all the good browsers won't accept it. So you will see at the beginning a warning.

The first thing you need to do with this API, you need to create an user account. So first go to the route
`https://127.0.0.1/user/signup`

The payload should consist of:
{
"username": "YOURUSERNAME",
"password": "YOURPASSWORD"
}
The API will return you token which will be valid for 10 hours (only because development, typicaly much shorter period).

Then you need to add this to the header:
Authorization: Bearer YOURTOKEN

## Testing
Backend Tests

`cd backend`

`pytest`

## Contributions

Feel free to fork the project, create a feature branch, and submit a pull request. Ensure that your code has proper comments and passes all the existing tests.

