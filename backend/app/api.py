from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from enum import Enum
from pydantic import BaseModel
from typing import Optional, Set, List
from src.query_request import DbQuery
from datetime import datetime
from config.constants import MISC
import os

####################

class JobLevel(str, Enum):
	intern = "Internship"
	entry = "Entry"
	middle = "Middle"
	senior = "Senior"
	other = "Other"

class JobLocation(str, Enum):
    berlin = "Berlin"
    munich = "Munich"
    hamburg = "Hamburg"
    cologne = "Cologne"


class JobRequest(BaseModel):
  # keyword: Optional[str] = None
  level: Optional[List[JobLevel]] = []
  location: Optional[List[JobLocation]] = []
  age: Optional[int] = 1
  order: Optional[str] = 'asc'
  page: Optional[int] = 1
  items_per_page: Optional[int] = MISC["items_per_page"]

####################

# def get_data_from_db():

def get_full_uri(route: str, req: JobRequest):
  url_params = []
  for key, value in req.dict().items():
      if value is not None:
        param = f"{key}={value}"
        url_params.append(param)
      else:
         continue


  return route + "?" + "&".join(url_params)


####################

####################

mongo_uri = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
db = DbQuery(mongo_uri)

origins = [
    "http://localhost:3000",  # React's default port
    "http://127.0.0.1:3000",
]

api = FastAPI()
api.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@api.get("/health")
def read_health():
    return {"status": "1"}

@api.get('/jobs')
def get_jobs(keyword:str, level:str, age:int, order:str = 'asc', page:int=1, items_per_page:int=10, location:List[JobLocation] = []):
  jobs = db.query_jobs(
    # keyword = keyword,
    level = level,
    location = location,
    age = age,
    order = order,
    page = page,
    items_per_page = items_per_page
  )

  # full_uri = get_full_uri('/jobs', req)

  result = {
    "meta": {
      "datetime": datetime.now()
      # "url": full_uri
    },
    "data": jobs
  }

  return result

@api.post('/jobs')
def post_jobs(req: JobRequest):
  jobs = db.query_jobs(
    # keyword = req.keyword,
    level = req.level,
    location = req.location,
    age = req.age,
    order = req.order,
    page = req.page,
    items_per_page = req.items_per_page
  )

  full_uri = get_full_uri('/jobs', req)

  result = {
    "meta": {
      "datetime": datetime.now(),
      "url": full_uri
    },
    "data": jobs
  }

  return result
  # return jobs