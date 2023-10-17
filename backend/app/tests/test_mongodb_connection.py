import pytest
import mongomock
from app.config.mongodb_connection import MongoDBUploader 
import json

# Fixture to create a MongoDBUploader instance and set it up for testing
@pytest.fixture
def mongo_uploader():
    """
    A pytest fixture that creates an instance of MongoDBUploader with a mock MongoDB client
    for testing purposes. The fixture yields the uploader instance for testing and ensures
    the connection is closed after the tests are done.
    """
    # Creating an instance of MongoDBUploader
    uploader = MongoDBUploader("testdb", "testcollection")
    
    # Using mongomock to create a mock MongoDB client for testing
    uploader.client = mongomock.MongoClient()
    
    # Setting the database and collection for the uploader
    uploader.db = uploader.client["testdb"]
    uploader.collection = uploader.db["testcollection"]
    
    yield uploader  # Yielding the uploader instance for testing
    
    uploader.close_connection()  # Closing the connection after tests

# Fixture to create a temporary JSON file for testing
@pytest.fixture
def json_file(tmp_path):
    """
    A pytest fixture that creates a temporary JSON file with sample data for testing.
    It yields the path to the temporary file.
    """
    # Sample data to write to the JSON file
    data = {"key": "value"}
    
    # Creating a temporary JSON file
    file_path = tmp_path / "data.json"
    
    with open(file_path, "w") as f:
        json.dump(data, f)  # Writing sample data to the JSON file
    
    return str(file_path)  # Returning the path to the temporary file as a string

# Test class containing all test methods for MongoDBUploader
class TestMongoDBUploader:
    
    def test_initialization(self, mongo_uploader):
        """
        Test to ensure that the MongoDBUploader is initialized with the correct database and collection names.
        """
        assert mongo_uploader.db.name == "testdb"
        assert mongo_uploader.collection.name == "testcollection"

    def test_upload_json_file(self, mongo_uploader, json_file):
        """
        Test to verify that a JSON file can be uploaded to MongoDB and that the data is stored correctly.
        """
        mongo_uploader.upload_json_file(json_file)  # Uploading the JSON file to MongoDB
    
        # Retrieving the stored data from MongoDB
        data = next(mongo_uploader.collection.find({}))
    
        assert data["key"] == "value"  # Verifying that the data is stored correctly

    def test_close_connection(self, mongo_uploader):
        """
        Test to verify that the MongoDB connection is closed properly.
        """
        mongo_uploader.close_connection()  # Closing the connection
    
        # Attempting to query the database after the connection is closed should raise an exception
        with pytest.raises(Exception):
            next(mongo_uploader.collection.find({}))

    def test_invalid_db_or_collection_names(self):
        """
        Test to ensure that initializing MongoDBUploader with invalid database or collection names
        (not strings) raises a TypeError.
        """
        # These initializations should raise a TypeError because the database and collection names are not strings
        with pytest.raises(TypeError):
            MongoDBUploader(123, "testcollection")
    
        with pytest.raises(TypeError):
            MongoDBUploader("testdb", 123)