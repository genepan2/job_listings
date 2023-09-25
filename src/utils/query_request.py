from pymongo import MongoClient
from typing import List, Optional
from datetime import datetime, timedelta
from config.constants import COLLECTIONS, MONGO, FIELDS, MISC

class DbQuery:
    def __init__(self, uri: str = MONGO["uri"], database_name: str = MONGO["db"], collection_name: str = COLLECTIONS["all"]):
        self.client = MongoClient(uri)
        self.database = self.client[database_name]
        self.collection = self.database[collection_name]

    def query_jobs(self, keyword: Optional[str] = None, level: Optional[str] = None,
                   location: Optional[str] = None, age: Optional[int] = None,
                   order: str = 'asc', page: int = 1, items_per_page: int = MISC["items_per_page"]) -> List[dict]:
        query = {}

        # print(self.calculate_date_from_age(age))

        if keyword:
            query[FIELDS["description"]] = {"$regex": keyword, "$options": "i"}

        if level:
            query[FIELDS["level"]] = self.map_level(level)

        if location:
            query[FIELDS["location"]] = location

        if age:
            query[FIELDS["publish_date"]] = {"$gte": self.calculate_date_from_age(age)}

        # Sort order
        # sort_order = pymongo.ASCENDING if order == 'asc' else pymongo.DESCENDING
        sort_order = 1 if order == 'asc' else -1

        # Calculate skip and limit for pagination
        offset = (page - 1) * items_per_page
        limit = items_per_page

        # Perform the query
        # result = self.collection.find(query, {"_id": 0}).sort(FIELDS["publish_date"], sort_order).skip(offset).limit(limit)
        result = self.collection.find({}, {"_id": 0}).sort(FIELDS["publish_date"], sort_order).skip(offset).limit(limit)

        # Convert query result to a list of dictionaries
        job_list = list(result)

        return job_list

    def map_level(self, level):
        if level == 'junior':
            return "Junior Level"
        elif level == 'mid':
            return "Mid Level"
        elif level == 'senior':
            return "Senior Level"

    def map_location(self, location: str):
        location = location.lower()

        if location == 'berlin':
            return "Berlin, Germany"
        elif location == 'munich':
            return "Munich, Germany"
        elif location == 'hamburg':
            return "Hamburg, Germany"
        elif location == 'cologne':
            return "Cologne, Germany"

    def calculate_date_from_age(self, age_in_days: int) -> datetime:
        current_date = datetime.now()
        calculated_date = current_date - timedelta(days=age_in_days)
        return calculated_date

    def close_connection(self):
        self.client.close()

