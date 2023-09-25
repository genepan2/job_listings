from fastapi import FastAPI, Header, HTTPException, Request
from enum import Enum
from pydantic import BaseModel
from typing import Optional, Set, List
from src.utils.query_request import DbQuery
from datetime import datetime
from config.constants import MISC


####################

class JobLevel(str, Enum):
    junior = "junior"
    mid = "mid"
    senior = "senior"

class JobLocation(str, Enum):
    berlin = "Berlin"
    munich = "Munich"
    hamburg = "Hamburg"
    cologne = "Cologne"


class JobRequest(BaseModel):
  keyword: Optional[str] = None
  level: Optional[JobLevel] = None
  location: Optional[JobLocation] = None
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

api = FastAPI()
db = DbQuery()

@api.post('/jobs')
def post_jobs(req: JobRequest):
  jobs = db.query_jobs(
    keyword = req.keyword,
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