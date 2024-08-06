from pydantic import BaseModel, Field
from pymongo import MongoClient
from dataclasses import dataclass
from enum import Enum


class DBType(Enum):
    MONGO = 'mongo'
    #POSTGRES = 'postgres'
    #SQLITE = 'sqlite'

@dataclass
class DBConfig(BaseModel):
    db_type: DBType = Field(default=DBType.MONGO)
    uri: str
    host: str = Field(default="localhost")
    port: int = Field(default=27017)
    database: str
    collection: str
    username: str = None
    password: str = None

def get_mongodb_client(config: DBConfig) -> MongoClient:
    client = MongoClient(config.uri, username=config.username, password=config.password)
    return client

def get_mongodb_collection(config: DBConfig):
    client = get_mongodb_client(config)
    db = client[config.collection]
    collection = db[config.collection]
    return collection