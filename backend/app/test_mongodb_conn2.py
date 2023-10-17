from pymongo import MongoClient
import sys

def list_collections_in_database(host='mongo', port=27017, db_name='job_listing_db'):
    """
    Connect to MongoDB and list all collections in a specific database.

    :param host: MongoDB host (default is localhost).
    :param port: MongoDB port (default is 27017).
    :param db_name: The name of the database to list collections from.
    """
    client = None  # Declare client at this scope so it can be closed in 'finally' block
    try:
        # Creating a client connection
        client = MongoClient(host, port, serverSelectionTimeoutMS=5000)  # 5 second timeout

        # Connecting to the database
        db = client[db_name]

        # Listing all collections in the "asd" database
        collection_list = db.list_collection_names()

        # Check if the list is empty
        if not collection_list:
            print(f"No collections found in the '{db_name}' database.")
        else:
            print(f"Collections in the '{db_name}' database:")
            for collection in collection_list:
                print(f"- {collection}")

    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
        sys.exit(1)  # Exit the script with an error code

    finally:
        # Close the connection
        if client:
            client.close()


# Call the function with default parameters (or specify your own)
list_collections_in_database()
