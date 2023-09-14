from pymongo import MongoClient

def merge_collections_to_all_jobs_list(mongo_uri='mongodb://localhost:27017/'):
    """
    Merge the contents of 'themuse_jobs_collected' and 'whatjobs_jobs_collected'
    into a new collection 'all_jobs_list'.
    """
    client = MongoClient(mongo_uri)
    db = client["job_listing_db"]

    # Create 'all_jobs_list' collection (if it doesn't exist)
    # If it exists, you might want to drop it first to avoid duplicate entries
    if "all_jobs_list" in db.list_collection_names():
        db["all_jobs_list"].drop()

    # Insert documents from 'themuse_jobs_collected' into 'all_jobs_list'
    themuse_jobs = db["themuse_jobs_collected"].find()
    db["all_jobs_list"].insert_many(list(themuse_jobs))

    # Insert documents from 'whatjobs_jobs_collected' into 'all_jobs_list'
    whatjobs_jobs = db["whatjobs_jobs_collected"].find()
    db["all_jobs_list"].insert_many(list(whatjobs_jobs))

    # Insert documents from 'linkedin_jobs_collected' into 'all_jobs_list'
    linkedin_jobs = db["linkedin_jobs_collected"].find()
    db["all_jobs_list"].insert_many(list(linkedin_jobs))

    # Get the total number of documents in the 'all_jobs_list' collection
    total_docs = db["all_jobs_list"].count_documents({})

    print(f"Merged both collections into 'all_jobs_list'. Total documents: {total_docs}")

    client.close()

# If you want to run this directly
if __name__ == "__main__":
    merge_collections_to_all_jobs_list()
