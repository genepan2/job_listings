# Job Listing README

This repository contains a Python script to scrape job details from different sources based on a user-provided keyword. The script collects job IDs for the given keyword, retrieves detailed information for each job, and saves the data to a CSV file with a unique name. Additionally, the script eliminates any blank rows from the CSV file and displays the job details as a DataFrame.

# Prerequisites
Before running the script, ensure you have the following:

Python 3.x installed on your system.
Required Python libraries: requests, beautifulsoup4, and pandas. You can install them using pip:

```pip install requests beautifulsoup4 pandas```

# How to Use
The script will prompt you to enter a keyword for the job search. Input the desired keyword and press Enter.

The script will proceed to scrape job details from LinkedIn based on the provided keyword.

Once the scraping is complete, the script will display the job details in a tabular format.

The data will be saved to a CSV file with a unique name, such as 'jobs1.csv', 'jobs2.csv', and so on. The file will be created in the same directory as the script.

# Important Notes
The script may take some time to complete, depending on the number of job listings retrieved from LinkedIn.

The User-Agent header is provided in the script to simulate a request from a web browser. However, LinkedIn's website structure might change over time, so there is a possibility that the script may stop working if LinkedIn updates its code.

This script is intended for educational and personal use only. Be mindful of LinkedIn's terms of service and don't abuse or overuse the scraping functionality.

# License
This script is provided under the MIT License. You are free to modify and use it as per the license terms.

# Contributions
If you find any issues or have suggestions to improve the script, feel free to create an issue or submit a pull request.

Happy job scraping!
