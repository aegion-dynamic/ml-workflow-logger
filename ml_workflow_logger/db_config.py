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

def get_mongodb_client(config: DBConfig) -> MongoClient:
    return MongoClient(config.uri)

def get_mongodb_collection(config: DBConfig):
    client = get_mongodb_client(config)
    db = client[config.collection]
    return db[config.collection]

# Example Usage:
if __name__ == "__main__":
    config = DBConfig(database="your_database_name", collection="your_collection_name")
    collection = get_mongodb_collection(config)
    print(f"Connected to collection: {config.collection}")