import pandas as pd
from pymongo import MongoClient
import os

class IntegrateCollections:
    @staticmethod
    def integrate_to_all_jobs_list(mongo_uri='mongodb://localhost:27017/'):
        """
        Merge the contents of 'themuse_jobs_collected', 'whatjobs_jobs_collected',
        and 'linkedin_jobs_collected' into a CSV file 'all_jobs_list.csv'.
        """
        client = MongoClient(mongo_uri)
        db = client["job_listing_db"]

        # Retrieve documents from 'themuse_jobs_collected'
        themuse_jobs = list(db["themuse_jobs_collected"].find())

        # Retrieve documents from 'whatjobs_jobs_collected'
        whatjobs_jobs = list(db["whatjobs_jobs_collected"].find())

        # Retrieve documents from 'linkedin_jobs_collected'
        linkedin_jobs = list(db["linkedin_jobs_collected"].find())

        # Combine all job listings into one list
        all_jobs_list = themuse_jobs + whatjobs_jobs + linkedin_jobs

        # Convert the list of dictionaries into a pandas DataFrame
        df = pd.DataFrame(all_jobs_list)

        # Specify the directory and file path
        directory = "backend/app/data/processed/integrated_data"
        csv_file_path = os.path.join(directory, "all_jobs_list.csv")

        # Check if the directory exists, if not, create it
        if not os.path.exists(directory):
            os.makedirs(directory)

        # Save the DataFrame to a CSV file
        df.to_csv(csv_file_path, index=False, encoding='utf-8')

        # Print the total number of job listings saved to the CSV file
        print(f"Saved {len(all_jobs_list)} job listings to '{csv_file_path}'.")

        client.close()

# Run the integration process
if __name__ == "__main__":
    IntegrateCollections.integrate_to_all_jobs_list()
