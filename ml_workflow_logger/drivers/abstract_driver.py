from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict

from ml_workflow_logger.flow import Flow
from ml_workflow_logger.models.flow_model import FlowModel, StepModel
from ml_workflow_logger.models.flow_record_model import FlowRecordModel
from ml_workflow_logger.models.run_model import RunModel
from ml_workflow_logger.run import Run

class DBType(Enum):
    MONGO = 'mongo'
    POSTGRES = 'postgres'
    SQLITE = 'sqlite'

@dataclass
class DBConfig:
    computed_connection_uri: str = field(init=False)
    database: str
    collection: str    
    db_type: DBType = DBType.MONGO
    host: str = "localhost"
    port: int = 27017    
    username: str = "root"
    password: str = "password"

    def __post_init__(self) -> None:
        """Post init method to set the uri based on the db_type

        Raises:
            ValueError: If the db_type is not supported
        """
        if self.db_type == DBType.MONGO:
            self.computed_connection_uri = f"mongodb://{self.username}:{self.password}@{self.host}:{self.port}/"
        else:
            raise ValueError(f"Unsupported DB type: {self.db_type}")


class AbstractDriver(ABC):

    def __init__(self):
        raise NotImplementedError("Subclasses must implement this method")

    @abstractmethod
    def save_flow(self, flow_data: Flow) -> None:
        raise NotImplementedError("Subclasses must implement this method")
    
    @abstractmethod
    def save_run(self, run_data: Run) -> None:
        raise NotImplementedError("Subclasses must implement this method")
    
    @abstractmethod
    def save_step(self, step_data: StepModel) -> None:
        raise NotImplementedError("Subclasses must implement this method")
    
    @abstractmethod
    def save_flow_record(self, flow_record_data: FlowRecordModel) -> None:
        raise NotImplementedError("Subclasses must implement this method")
    
    @abstractmethod
    def save_params(self, run_id: str, params: Dict[str, Any]) -> None:
        raise NotImplementedError("Subclasses must implement this method")
    
    @abstractmethod
    def save_metrics(self, run_id: str, metrics: Dict[str, Any]) -> None:
        raise NotImplementedError("Subclasses must implement this method")