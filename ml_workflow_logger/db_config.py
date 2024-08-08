from pymongo import MongoClient
#from dataclasses import dataclass
from enum import Enum


class DBType(Enum):
    MONGO = 'mongo'
    POSTGRES = 'postgres'
    SQLITE = 'sqlite'

#@dataclass
class DBConfig:
    db_type: DBType = DBType.MONGO
    uri: str
    host: str = "localhost"
    port: int = 27017
    database: str
    collection: str
    username: str = None
    password: str = None

# def get_mongodb_client(config: DBConfig) -> MongoClient:
#     client = MongoClient(config.uri, username=config.username, password=config.password)
#     return client

def get_mongodb_collection(config: DBConfig):
    client = MongoClient(config.uri)
    db = client[config.collection]
    collection = db[config.collection]
    return db[config.collection]