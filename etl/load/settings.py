import os
import sys

# Absolute path to the config directory
CONFIG_PATH = "../job_listing/config"

# Add the config directory to the system path for imports
sys.path.insert(0, CONFIG_PATH)

# Path to the WhatJobs cleaned data
WHATJOBS_FILE_PATH = os.path.join("../job_listing", "data", "processed", "whatjobs_json_files", "whatjobs_cleaned_data.json")

# Path to the Themuse cleaned data
THEMUSE_FILE_PATH = os.path.join("../job_listing", "data", "processed", "themuse_json_files", "themuse_cleaned_data.json")

# Path to the LinkedIn cleaned data
LINKEDIN_FILE_PATH = os.path.join("../job_listing", "data", "processed", "linkedin_json_files", "linkedin_cleaned_data.json")