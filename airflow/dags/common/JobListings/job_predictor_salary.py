from airflow.exceptions import AirflowException

import pymongo

import os
import logging

import joblib
import pandas as pd
import numpy as np
from sklearn.preprocessing import OneHotEncoder

from job_constants import PATH, COLLECTIONS
import job_helper_database as JobHelperDatabase


class JobPredictorSalary:
    def __init__(self, source_name):
        logging.info(source_name)
        # self.model_path = 'backend/app/ml/reglog_model.pkl'
        self.model_path = os.path.join(
            PATH["ml"], 'salary', 'reglog_model.pkl')
        self.source_name = source_name

        # self.job_listing_data_path = 'backend/app/data/processed/integrated_data/all_jobs_list.csv'
        # self.job_listing_data_path = os.path.join(PATH_DATA_PROCESSED, 'integrated_data', 'all_jobs_list.csv')

        # self.X_train_path = 'backend/app/ml/X_train.csv'
        self.X_train_path = os.path.join(PATH["ml"], 'salary', 'X_train.csv')

        # Load the model and data when an instance is created
        # self.loaded_model = joblib.load(self.model_path)

        try:
            # Check if the file exists
            if os.path.exists(self.model_path) and os.path.isfile(self.model_path):
                self.loaded_model = joblib.load(self.model_path)
            else:
                raise FileNotFoundError(
                    f"Model file does not exist at the specified path: {self.model_path}")
                # raise AirflowException(str(e))
        except Exception as e:
            # Handle other types of exceptions
            logging.info(f"An error occurred while loading the model: {e}")
            raise AirflowException(str(e))

        # self.job_listing_df = pd.read_csv(self.job_listing_data_path)
        # self.job_listing_df = get_data_from_source_collections(source_name)
        self.job_listing_df = pd.DataFrame(
            JobHelperDatabase.get_data_from_source_collections(COLLECTIONS[self.source_name]))
        # logging.info(type(self.job_listing_df))
        self.X_train = pd.read_csv(self.X_train_path)

    def one_hot_encode_data(self):
        # logging.info("Columns in the DataFrame: %s", self.job_listing_df.columns)
        job_listing_selected = self.job_listing_df[[
            'location', 'title', 'language', 'level']].copy()
        job_listing_encoded = pd.get_dummies(job_listing_selected)
        return job_listing_encoded

    def fill_missing_columns(self, job_listing_encoded):
        random_fill_values = np.random.randint(
            2, size=(len(job_listing_encoded), len(self.X_train.columns)))
        random_fill_df = pd.DataFrame(
            random_fill_values, columns=self.X_train.columns)
        job_listing_encoded = job_listing_encoded.reindex(
            columns=self.X_train.columns)
        job_listing_encoded.update(random_fill_df)
        return job_listing_encoded

    def predict_salaries(self, job_listing_encoded):
        predicted_salaries = self.loaded_model.predict(job_listing_encoded)
        return predicted_salaries

    def map_salary_to_class(self, salary):
        if salary == 'Class 1':
            return "Up to 60,000"
        elif salary == 'Class 2':
            return "Up to 80,000"
        elif salary == 'Class 3':
            return "Over 100,000"
        else:
            try:
                return int(salary)
            except ValueError:
                return None

    def predict_and_map_salaries(self, output_path=None):
        logging.info("predict_and_map_salaries")
        job_listing_encoded = self.one_hot_encode_data()
        job_listing_encoded = self.fill_missing_columns(job_listing_encoded)
        predicted_salaries = self.predict_salaries(job_listing_encoded)
        # logging.info('dataframe head - {}'.format(predicted_salaries.to_string()))
        self.job_listing_df['predicted_salary'] = [
            self.map_salary_to_class(s) for s in predicted_salaries]

        # Prepare bulk update operations
        updates = []
        for index, row in self.job_listing_df.iterrows():
            # This assumes you have an '_id' or some unique identifier in your DataFrame
            filter_doc = {'_id': row['_id']}
            # update_doc = {'$set': row.to_dict()}  # This will update all columns. Restrict to specific fields if necessary.
            update_doc = {
                '$set': {'predicted_salary': row['predicted_salary']}}
            logging.info(row['predicted_salary'])

            updates.append(pymongo.UpdateOne(filter_doc, update_doc))

        # Perform the bulk update
        if updates:
            # result = collection.bulk_write(updates)
            result = JobHelperDatabase.update_bulk_records(
                updates, self.source_name)

        # Check the outcome (optional)
        if result:
            logging.info(f"Matched counts: {result.matched_count}")
            logging.info(f"Modified counts: {result.modified_count}")

        # output_path = 'backend/app/data/processed/integrated_data/predicted_jobs_list.csv'

        # if output_path:
        #     self.job_listing_df.to_csv(output_path, index=False)

        # logging.info("Prediction completed and saved to", output_path)  # Add completion message

        # return self.job_listing_df
