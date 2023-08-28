from pymongo import MongoClient

def search_jobs_by_keyword(keyword):
    # Connect to MongoDB
    client = MongoClient('mongodb://localhost:27017/')
    db = client["job_listing_db"]
    collection = db["all_jobs_list"]

    # Query the database
    results = list(collection.find({"search_keyword": {"$regex": keyword, "$options": 'i'}}))

    # Print the total number of results
    total_results = len(results)
    print("-" * 50)
    print(f"Total results found: {total_results}\n")
    print("-" * 50)

    # Display the results with numbering
    for idx, job in enumerate(results, 1):
        print(f"Job Number-{idx}")
        print("Job Title:", job.get("job_title", "N/A"))
        print("Company Name:", job.get("company_name", "N/A"))
        print("Job Location:", job.get("job_location", "N/A"))
        print("Job Level:", job.get("job_level", "N/A"))
        print("Publication Date:", job.get("publication_date", "N/A"))
        print("Job URL:", job.get("job_url", "N/A"))
        #print("Job Description:", job.get("job_description", "N/A"))
        print("-" * 50)

    client.close()

def main():
    # Prompt user for job category
    options = {
        "1": "Data",
        "2": "Software Engineering NOT WORKIN YET"
    }
    print("Choose a job category to search for:")
    for key, value in options.items():
        print(f"{key} - {value}")

    # Get user input
    choice = input("Enter the number corresponding to your choice: ")

    # Map user input to job category
    category = options.get(choice)
    if not category:
        print("Invalid choice.")
        return

    # Search and display jobs based on the chosen category
    search_jobs_by_keyword(category)

if __name__ == "__main__":
    main()
