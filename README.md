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

## Setup and Installation

1. Clone the Repository
   `git clone https://github.com/leviGab001/job_listing`
2. Create the .env file from the provided .env_example
3. Build the Docker Images and run start the containers
   `docker compose up --build`
4. Wait till all container run and are healthy (Screenshot?)
5. Open the UI in your browser:
   `http://localhost:3000`
6. Open Airflow to see the orchestration:
   `http://localhost:8080`

## Testing

Backend Tests

`cd backend/tests`
`pytest`

## Contributions

Feel free to fork the project, create a feature branch, and submit a pull request. Ensure that your code has proper comments and passes all the existing tests.

## License

This project is licensed under the MIT License.
