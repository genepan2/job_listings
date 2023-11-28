# README

## Description

### Overview

This project is a culmination of the skills and knowledge acquired during the [Datascientest](https://github.com/DataScientest) Bootcamp, showcasing our ability to apply data engineering principles in a real-world scenario, beyond the confines of structured learning modules.

### Project Objective

The primary objective of this project is to demonstrate a comprehensive understanding of data engineering by aggregating job postings from diverse sources, namely TheMuse, WhatJobs, and LinkedIn. Our approach involves a meticulous process of data cleaning and transformation, ensuring the information is both accurate and useful.

### Key Features

- **Data Aggregation:** Collates job postings from multiple platforms to provide a broad perspective of the job market.
- **Data Transformation:** Employs advanced techniques to clean and convert data into a structured and analyzable format.
- **User-Friendly Interface:** Presents the processed data through an intuitive and accessible interface, enhancing user experience.
- **Automation:** Streamlines the entire process, from data collection to presentation, ensuring efficiency and consistency.
- **Machine Learning Integration:** Utilizes machine learning algorithms to predict job salaries, adding a predictive dimension to our data analysis.

### Technologies

Backend: Python, FastAPI
Frontend: ReactJS, TailwindCSS
Workflow Management: Airflow
Database: MongoDB
Machine Learning: Sklearn
Containerization: Docker
Uni Test: Pytest
Deployment: Git Action, Docker Hub, AWS

### System Design

![system_design](https://github.com/leviGab001/job_listings/blob/main/images/system_design.png)

## Setup and Installation

1. Clone the Repository

   - Run command: `git clone https://github.com/leviGab001/job_listing`
   - Navigate to the project directory: `cd job_listing`

2. Create the .env File

   - Copy the file: `cp .env.example .env.`
   - Modify the environment variables

3. Build Docker Images and Start Containers

   - Run command: `docker compose -f docker-compose.dev.yml up --build`
   - All 9 containers should start. To check if all are running and are healthy run `docker compose -f docker-compose.dev.yml ps`
     ![job-listings--screenshot--docker-containers-healthy](https://github.com/leviGab001/job_listings/assets/10182052/388e0a02-7899-41da-8492-92f8c26518e8)

4. Access the Application UI

   - Open your web browser and go to `http://localhost:3000` to access the application's user interface.

5. Access Airflow
   - Open your web browser and go to `http://localhost:8080`
   - The default credentials are `airflow/airflow`. You can change them in the .env file.

## Unit Tests

GitHub Actions CI/CD pipeline workflow is configured to automatically execute unit tests whenever changes are pushed or a pull request is made to the main branch. This approach ensures that any changes introduced into the codebase do not break existing functionality and adhere to expected behaviors.

**Test Scripts**

**test_api.py**

This script contains unit tests for the FastAPI application. It tests the API endpoints to ensure they return the expected data and status codes. Key features tested include:

- Mocking database queries to isolate the API layer.
- Testing the GET request to /jobs endpoint.
- Ensuring the API returns the correct response and status code.
- File Location: `/backend/app/tests/test_api.py`

**test_mongodb_connection.py**

This script focuses on testing the MongoDB connection and operations, particularly for the MongoDBUploader class. It includes:

- A pytest fixture to create a mock instance of MongoDBUploader.
- Use of mongomock to simulate a MongoDB environment for testing.
- Tests to ensure proper setup and operations of the MongoDB connection and data handling functions.
- File Location: `/backend/app/tests/test_mongodb_connection.py`

## Deployment

**CI/CD Pipeline with GitHub Actions** [![CI/CD Pipeline](https://github.com/leviGab001/job_listings/actions/workflows/pipeline.yml/badge.svg?branch=main)](https://github.com/leviGab001/job_listings/actions/workflows/pipeline.yml)


![cicd pipeline](https://github.com/leviGab001/job_listings/blob/main/images/git_actions.png)


The pipeline is designed for robustness, ensuring that **new deployments only occur after successful unit tests.**

**Environmental variables and secrets** (like AWS credentials and SSH keys) are securely used for authenticating and accessing necessary resources.

The deployment process is fully automated, reducing the risk of human error and ensuring consistent setups.

**Steps for Deployment**

**Job Dependency:** The deployment job **'build-push-deploy'** waits for the successful completion of the **'unit-test'** job before it starts.

**Docker Image Build and Push:**

- Builds Docker images for various components of the application using docker-compose.
- Tags and pushes these images to a Docker registry.

**Deployment to Server:**

- Uses `appleboy/ssh-action` to SSH into the server.
- Sequentially deploys several Docker images, including MongoDB, API, Frontend, Postgres, and Redis.
- Each deployment step involves removing any existing container, pulling the latest image, and running the container with the appropriate configurations.

**Security and Best Practices**

- All sensitive credentials are managed through GitHub secrets, ensuring security and confidentiality.
- The deployment process is modular, allowing for independent updating of different components of the application.

## Contributions

Feel free to fork the project, create a feature branch, and submit a pull request. Ensure that your code has proper comments and passes all the existing tests.

## License

This project is licensed under the MIT License.
