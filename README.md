# README

## Project Overview 🌐

The job_listing project is a comprehensive solution for job listing, retrieval, and management. It consists of a backend application for data processing, an Airflow setup for workflow management, a frontend application for user interaction, and MongoDB for data storage. Salary Prediction with Classification Model. The project is containerized using Docker for easy deployment and scalability. 

## 🛠️Technologies

Backend: Python

Frontend: ReactJS

Workflow Management: Airflow

Database: MongoDB

Machine Learning: Sklearn

Containerization: Docker

Uni Test: Pytest

Deployment: Git Action, Docker Hub, AWS


## 🔧Setup and Installation

Clone the Repository

`git clone https://github.com/leviGab001/job_listing`

`cd job_listing`

Build and Start the Docker Containers

`docker compose up --build`

## 🚀How to Use it

Frontend: Open a web browser and navigate to `http://localhost:3000`

Backend API: Send requests to `http://localhost:8000`


## 🧪Unit Tests

GitHub Actions CI/CD pipeline workflow is configured to automatically execute unit tests whenever changes are pushed or a pull request is made to the main branch. This approach ensures that any changes introduced into the codebase do not break existing functionality and adhere to expected behaviors.

**Test Scripts**

**test_api.py** 

This script contains unit tests for the FastAPI application. It tests the API endpoints to ensure they return the expected data and status codes. Key features tested include:

* Mocking database queries to isolate the API layer.
* Testing the GET request to /jobs endpoint.
* Ensuring the API returns the correct response and status code.
* File Location: `/backend/app/tests/test_api.py`

**test_mongodb_connection.py** 

This script focuses on testing the MongoDB connection and operations, particularly for the MongoDBUploader class. It includes:

* A pytest fixture to create a mock instance of MongoDBUploader.
* Use of mongomock to simulate a MongoDB environment for testing.
* Tests to ensure proper setup and operations of the MongoDB connection and data handling functions.
* File Location: ``/backend/app/tests/test_mongodb_connection.py``

## 🛫Deployment 

**CI/CD Pipeline with GitHub Actions** 


[![CI/CD Pipeline](https://github.com/leviGab001/job_listings/actions/workflows/pipeline.yml/badge.svg?branch=main)](https://github.com/leviGab001/job_listings/actions/workflows/pipeline.yml)

The pipeline is designed for robustness, ensuring that **new deployments only occur after successful unit tests.**

**Environmental variables and secrets** (like AWS credentials and SSH keys) are securely used for authenticating and accessing necessary resources.

The deployment process is fully automated, reducing the risk of human error and ensuring consistent setups.


**Steps for Deployment**

**Job Dependency:** The deployment job **'build-push-deploy'** waits for the successful completion of the **'unit-test'** job before it starts.

**Docker Image Build and Push:**
* Builds Docker images for various components of the application using docker-compose.
* Tags and pushes these images to a Docker registry.

**Deployment to Server:**
* Uses ``appleboy/ssh-action`` to SSH into the server.
* Sequentially deploys several Docker images, including MongoDB, API, Frontend, Postgres, and Redis.
* Each deployment step involves removing any existing container, pulling the latest image, and running the container with the appropriate configurations.

**Security and Best Practices**
* All sensitive credentials are managed through GitHub secrets, ensuring security and confidentiality.
* The deployment process is modular, allowing for independent updating of different components of the application.



## 🤝Contributions

Feel free to fork the project, create a feature branch, and submit a pull request. Ensure that your code has proper comments and passes all the existing tests.

## 📜 License
This project is licensed under the MIT License.
