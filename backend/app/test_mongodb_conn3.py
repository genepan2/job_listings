from pymongo import MongoClient
import sys

def fetch_documents_from_collection(host='mongo', port=27017, db_name='job_listing_db', collection_name='jobs_all'):
    """
    Connect to MongoDB, select a database and a collection, then fetch and display documents from that collection.

    :param host: MongoDB host (default is localhost).
    :param port: MongoDB port (default is 27017).
    :param db_name: The name of the database to select.
    :param collection_name: The name of the collection to fetch documents from.
    """
    client = None  # Declare client at this scope so it can be closed in 'finally' block
    try:
        # Creating a client connection
        client = MongoClient(host, port, serverSelectionTimeoutMS=5000)  # 5 second timeout

        # Connecting to the database
        db = client[db_name]

        # Accessing the collection
        collection = db[collection_name]

        # Counting documents in the collection (optional step to verify documents exist)
        count = collection.count_documents({})
        if count == 0:
            print(f"No documents found in the '{collection_name}' collection.")
            return  # No documents to fetch, exit the function

        # Fetching documents from the collection
        documents = collection.find()

        # Print each document in the collection
        print(f"Documents in the '{collection_name}' collection within '{db_name}' database:")
        for doc in documents:
            print(doc)

    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
        sys.exit(1)  # Exit the script with an error code

    finally:
        # Close the connection
        if client:
            client.close()


# Call the function with default parameters (or specify your own)
fetch_documents_from_collection()
