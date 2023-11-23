from pymongo import MongoClient
from typing import List, Optional
from datetime import datetime, timedelta
# from app.config.constants import COLLECTIONS, MONGO, FIELDS, MISC
from config.constants import COLLECTIONS, MONGO, FIELDS, MISC

class DbQuery:
    def __init__(self, uri: str = MONGO["uri"], database_name: str = MONGO["db"], collection_name: str = COLLECTIONS["all"]):
        self.client = MongoClient(uri)
        self.uri = uri
        self.database = self.client[database_name]
        self.collection = self.database[collection_name]

    def query_jobs(self, keyword: Optional[str] = None, level: Optional[str] = None,
                   location: Optional[str] = None, language: Optional[str] = None,
                   age: Optional[int] = None,
                   order: str = 'desc', page: int = 1, items_per_page: int = MISC["items_per_page"]) -> List[dict]:
        query = {}

        # print(self.calculate_date_from_age(age))
        '''
        if keyword:
            query[FIELDS["description"]] = {"$regex": keyword, "$options": "i"}


        if age:
            query[FIELDS["publish_date"]] = {"$gte": self.calculate_date_from_age(age)}
        '''

        # if level:
        #     query[FIELDS["level"]] = self.map_level(level)
        if level:
            if isinstance(level, list):
                query[FIELDS["level"]] = {"$in": [lev for lev in level]}
            else:
                query[FIELDS["level"]] = level

        # if location:
        #     query[FIELDS["location"]] = location
        if location:
            if isinstance(location, list):
                query[FIELDS["location"]] = {"$in": [loc for loc in location]}
            else:
                query[FIELDS["location"]] = location

        if language:
            if isinstance(language, list):
                query[FIELDS["language"]] = {"$in": [lang for lang in language]}
            else:
                query[FIELDS["language"]] = language


        # Sort order
        # sort_order = pymongo.ASCENDING if order == 'asc' else pymongo.DESCENDING
        sort_order = 1 if order == 'asc' else -1

        # Calculate skip and limit for pagination
        offset = (page - 1) * items_per_page
        limit = items_per_page

        # Perform the query
        result = self.collection.find(query, {"_id": 0}).sort(FIELDS["publish_date"], sort_order).skip(offset).limit(limit)
        # result = self.collection.find({}, {"_id": 0}).sort(FIELDS["publish_date"], sort_order).skip(offset).limit(limit)


        # Now, create an aggregation pipeline that includes the current query for stats calculation.
        stats_pipeline = [
            {"$match": query},  # This line ensures stats are calculated based on the current query
            {
                "$facet": {
                    "levels": [{"$group": {"_id": "$level", "count": {"$sum": 1}}}],
                    "languages": [{"$group": {"_id": "$language", "count": {"$sum": 1}}}],
                    "locations": [{"$group": {"_id": "$location", "count": {"$sum": 1}}}],
                }
            }
        ]

        # stats_result = self.collection.aggregate(stats_pipeline)
        aggregate_result = list(self.collection.aggregate(stats_pipeline))
        facet_result = aggregate_result[0]

        levels_stats = {entry["_id"]: entry["count"] for entry in facet_result["levels"]}
        languages_stats = {entry["_id"]: entry["count"] for entry in facet_result["languages"]}
        locations_stats = {entry["_id"]: entry["count"] for entry in facet_result["locations"]}

        stats = {
            "levels": levels_stats,
            "languages": languages_stats,
            "locations": locations_stats,
        }

        data = list(result)
        # stats = {entry["_id"]: entry["count"] for entry in stats_result}

        return {"data": data, "stats": stats}

    def calculate_date_from_age(self, age_in_days: int) -> datetime:
        current_date = datetime.now()
        calculated_date = current_date - timedelta(days=age_in_days)
        return calculated_date

    def close_connection(self):
        self.client.close()

