import joblib
import pandas as pd
import numpy as np
from sklearn.preprocessing import OneHotEncoder

class SalaryPredictor:
    def __init__(self):
        # Define the paths to your model and data files
        self.model_path = 'backend/app/ml/reglog_model.pkl'
        self.job_listing_data_path = 'backend/app/data/processed/integrated_data/all_jobs_list.csv'
        self.X_train_path = 'backend/app/ml/X_train.csv'

        # Load the model and data when an instance is created
        self.loaded_model = joblib.load(self.model_path)
        self.job_listing_df = pd.read_csv(self.job_listing_data_path)
        self.X_train = pd.read_csv(self.X_train_path)

    def one_hot_encode_data(self):
        job_listing_selected = self.job_listing_df[['location', 'title', 'language', 'level']].copy()
        job_listing_encoded = pd.get_dummies(job_listing_selected)
        return job_listing_encoded

    def fill_missing_columns(self, job_listing_encoded):
        random_fill_values = np.random.randint(2, size=(len(job_listing_encoded), len(self.X_train.columns)))
        random_fill_df = pd.DataFrame(random_fill_values, columns=self.X_train.columns)
        job_listing_encoded = job_listing_encoded.reindex(columns=self.X_train.columns)
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
        job_listing_encoded = self.one_hot_encode_data()
        job_listing_encoded = self.fill_missing_columns(job_listing_encoded)
        predicted_salaries = self.predict_salaries(job_listing_encoded)
        self.job_listing_df['predicted_salary'] = [self.map_salary_to_class(s) for s in predicted_salaries]

        output_path = 'backend/app/data/processed/integrated_data/predicted_jobs_list.csv'

        if output_path:
            self.job_listing_df.to_csv(output_path, index=False)

        print("Prediction completed and saved to", output_path)  # Add completion message

        return self.job_listing_df

if __name__ == "__main__":
    # Create an instance of SalaryPredictor
    salary_predictor = SalaryPredictor()
    
    # Call the predict_and_map_salaries 
    result_df = salary_predictor.predict_and_map_salaries()
