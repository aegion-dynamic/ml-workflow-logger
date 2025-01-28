import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict

import pandas as pd

from ml_workflow_logger.flow import Flow
from ml_workflow_logger.run import Run

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DBType(Enum):
    MONGO = "mongo"
    POSTGRES = "postgres"
    SQLITE = "sqlite"


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
        logger.info(f"Initialized DBConfig with DB type {self.db_type} and connection URI")


class AbstractDriver(ABC):
    """Abstract driver class defining the required method for database interaction."""

    def __init__(self):
        raise NotImplementedError("Subclasses must implement this method")

    @abstractmethod
    def save_flow(self, flow_object: Flow) -> None:
        """Saves the flow data"""
        raise NotImplementedError("Subclasses must implement this method")

    @abstractmethod
    def add_step(self, flow_id: str, step_name: str, step_data: Dict[str, Any] = {}) -> None:
        """Add the step"""
        raise NotImplementedError("Subclasses must implement this method")

    @abstractmethod
    def save_new_run(self, run_object: Run) -> None:
        """Saves the run data"""
        raise NotImplementedError("Subclasses must implement this method")

    @abstractmethod
    def save_metrics(self, run_id: str, metrics: Dict[str, Any]) -> None:
        """Saves the metrics for a given run."""
        raise NotImplementedError("Subclasses must implement this method")

    @abstractmethod
    def save_flow_record(self, run_id: str, flow_id: str, step_name: str, step_data: Dict[str, Any]) -> None:
        """Saves the flow record data."""
        raise NotImplementedError("Subclasses must implement this method")

    @abstractmethod
    def save_dataframe(self, run_id: str, df: pd.DataFrame) -> None:
        """Save a pandas DataFrame associated with a specific run."""
        raise NotImplementedError("Subclasses must implement this method")

    @abstractmethod
    def update_run_status(self, run_id: str, status: str) -> None:
        """Update the status of a specific run."""
        raise NotImplementedError("Subclasses must implement this method")

    @abstractmethod
    def update_flow_status(self, flow_id: str, status: str) -> None:
        """Update the status of a specific flow."""
        raise NotImplementedError("Subclasses must implement this method")
