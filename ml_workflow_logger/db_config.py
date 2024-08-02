from dataclasses import dataclass
from enum import Enum


class DBType(Enum):
    MONGO = 'mongo'
    POSTGRES = 'postgres'
    SQLITE = 'sqlite'

@dataclass
class DBConfig:
    db_type: DBType
    uri: str
    database: str
    collection: str