from pymongo import MongoClient
from dataclasses import dataclass, field
from enum import Enum


class DBType(Enum):
    MONGO = 'mongo'
    POSTGRES = 'postgres'
    SQLITE = 'sqlite'

@dataclass
class DBConfig:
    uri: str = field(init=False)
    database: str
    collection: str    
    db_type: DBType = DBType.MONGO
    host: str = "localhost"
    port: int = 27017    
    username: str = "root"
    password: str = "password"
    

    def __post_init__(self):
        if self.db_type == DBType.MONGO:
            self.uri = f"mongodb://{self.username}:{self.password}@{self.host}:{self.port}/"
        else:
            raise ValueError(f"Unsupported DB type: {self.db_type}")

def create_mongodb_client(config: DBConfig) -> MongoClient:
    return MongoClient(
        host=config.uri,
        username=config.username,
        password=config.password,
        port=config.port
    )

def get_mongodb_collection(config: DBConfig):
    client = create_mongodb_client(config)
    db = client[config.collection]
    return db[config.collection]

