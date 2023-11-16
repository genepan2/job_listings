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

## 🔄Stages

Data Collection

Data Processing

Data Consumption

Automation

Deployment

## 🔧Setup and Installation

Clone the Repository

`git clone https://github.com/leviGab001/job_listing`

`cd job_listing`

Build and Start the Docker Containers

`docker compose up --build`

## 🚀Access the Application

Frontend: Open a web browser and navigate to `http://localhost:3000`

Backend API: Send requests to `http://localhost:8000`

## 🔒How to Use the API (secure)

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

## 🧪Unit Tests
**Backend Tests**

GitHub Actions CI/CD (Continuous Integration/Continuous Delivery) workflow is configured to automatically execute unit tests whenever changes are pushed or a pull request is made to the main branch. This approach ensures that any changes introduced into the codebase do not break existing functionality and adhere to expected behaviors.

**Test Scripts**

**/backend/app/tests/test_api.py:** This script contains unit tests for the FastAPI application. It tests the API endpoints to ensure they return the expected data and status codes. Key features tested include:

* Mocking database queries to isolate the API layer.
* Testing the GET request to /jobs endpoint.
* Ensuring the API returns the correct response and status code.

**/backend/app/tests/test_mongodb_connection.py:** This script focuses on testing the MongoDB connection and operations, particularly for the MongoDBUploader class. It includes:

* A pytest fixture to create a mock instance of MongoDBUploader.
* Use of mongomock to simulate a MongoDB environment for testing.
* Tests to ensure proper setup and operations of the MongoDB connection and data handling functions.

## 🧪Deployment

**CI/CD Pipeline with GitHub Actions**

The pipeline is designed for robustness, ensuring that **new deployments only occur after successful unit tests.**
**Environmental variables and secrets** (like AWS credentials and SSH keys) are securely used for authenticating and accessing necessary resources.
The deployment process is fully automated, reducing the risk of human error and ensuring consistent setups.

**Deployment Process**

**Job Dependency:** The deployment job (build-push-deploy) waits for the successful completion of the unit-test job before it starts.

**Environment Variables:** Sets up necessary environment variables such as AWS credentials, server IP, and user details, securely stored in GitHub secrets.

**Steps for Deployment:**

**Repository Checkout:** The job begins by checking out the codebase.
**Docker Image Build and Push:**
* Builds Docker images for various components of the application using docker-compose.
* Tags and pushes these images to a Docker registry.
* Note: Airflow images are not deployed due to their size and impact on AWS ECS instances.
**Deployment to Server:**
* Uses appleboy/ssh-action to SSH into the server.
* Sequentially deploys several Docker images, including MongoDB, API, Frontend, Postgres, and Redis.
* Each deployment step involves removing any existing container, pulling the latest image, and running the container with the appropriate configurations.

**Security and Best Practices**
* All sensitive credentials are managed through GitHub secrets, ensuring security and confidentiality.
* The deployment process is modular, allowing for independent updating of different components of the application.



## 🤝Contributions

Feel free to fork the project, create a feature branch, and submit a pull request. Ensure that your code has proper comments and passes all the existing tests.

## 📜 License
This project is licensed under the MIT License.
