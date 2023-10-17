from pymongo import MongoClient
import sys

def test_mongo_connection(host='localhost', port=27017):
    """
    Test connection to MongoDB.

    :param host: MongoDB host (default is localhost).
    :param port: MongoDB port (default is 27017).
    """
    try:
        # Creating a client connection
        client = MongoClient(host, port, serverSelectionTimeoutMS=5000)  # 5 second timeout

        # The ismaster command is cheap and does not require auth.
        client.admin.command('ismaster')

        print("MongoDB connection successful.")
    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
        sys.exit(1)  # Exit the script with an error code

    finally:
        # Close the connection
        client.close()


# Call the function with default parameters (or specify your own)
test_mongo_connection()