from fastapi import FastAPI, Header, HTTPException, Request, Body, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import datetime
from enum import Enum
from src.query_request import DbQuery
from typing import Optional, Set, List
import jwt
import time
from config.constants import MISC

####################

JWT_SECRET = "secret"
JWT_ALGORITHM = "HS256"

users = []

####################

class UserSchema(BaseModel):
    username: str
    password: str
    is_admin: bool = False

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

def check_user(data: UserSchema):
    for user in users:
        if user.username == data.username and user.password == data.password:
            return True
    return False


def token_response(token: str):
    return {"access_token": token}


def sign_jwt(user_id: str):
    payload = {"user_id": user_id, "expires": time.time() + 6000}
    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

    return token_response(token)


def decode_jwt(token: str):
    try:
        decoded_token = jwt.decode(
            token, JWT_SECRET, algorithms=[JWT_ALGORITHM]
        )
        return (
            decoded_token if decoded_token["expires"] >= time.time() else None
        )
    except Exception:
        return {}

#####

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

class JWTBearer(HTTPBearer):
    def __init__(self, auto_error: bool = True):
        super(JWTBearer, self).__init__(auto_error=auto_error)

    async def __call__(self, request: Request):
        credentials: HTTPAuthorizationCredentials = await super(
            JWTBearer, self
        ).__call__(request)
        if credentials:
            if not credentials.scheme == "Bearer":
                raise HTTPException(
                    status_code=403, detail="Invalid authentication scheme."
                )
            if not self.verify_jwt(credentials.credentials):
                raise HTTPException(
                    status_code=403, detail="Invalid token or expired token."
                )
            return credentials.credentials
        else:
            raise HTTPException(
                status_code=403, detail="Invalid authorization code."
            )

    def verify_jwt(self, jwtoken: str):
        isTokenValid: bool = False

        try:
            payload = decode_jwt(jwtoken)
        except Exception:
            payload = None
        if payload:
            isTokenValid = True
        return isTokenValid

####################

db = DbQuery()

origins = [
    "http://localhost:3000",  # React's default port
    "http://127.0.0.1:3000",
    "https://localhost:3000",  # React's default port
    "https://127.0.0.1:3000",
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
# def get_jobs(keyword:str, level:str, location:str, age:int, order:str = 'asc', page:int=1, items_per_page:int=10):
def get_jobs(level:str, location:str, age:int, order:str = 'asc', page:int=1, items_per_page:int=10):
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

@api.post('/jobs', dependencies=[Depends(JWTBearer())])
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

@api.post("/user/signup")
async def user_create(user: UserSchema = Body(...)):
    users.append(user)
    return sign_jwt(user.username)


@api.post("/user/login")
async def user_login(user: UserSchema = Body(...)):
    if check_user(user):
        return sign_jwt(user.username)
    return {"error": "Wrong login details!"}