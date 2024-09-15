import threading
import logging
import networkx as nx
from typing import Any, Dict, Optional
from venv import logger
from ml_workflow_logger.flow import Flow
from ml_workflow_logger.models.flow_model import StepModel
from ml_workflow_logger.run import Run
from ml_workflow_logger.models.flow_record_model import FlowRecordModel
from ml_workflow_logger.drivers.mongodb import MongoDBDriver
from ml_workflow_logger.drivers.abstract_driver import AbstractDriver, DBConfig, DBType
import pandas as pd
#import select
# from pymongo import MongoClient
# from ml_workflow_logger.local_data_store import LocalDataStore


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MLWorkFlowLogger:
    _instance = None
    _lock =  threading.Lock()

    def __new__(cls) -> Any:
        """Ensure Singleton instance creation"""
        if cls._instance is None:
            with cls._lock: # Ensure thread safety
                if cls._instance is None:
                    cls._instance = object.__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    

    def __init__(self, db_driver: Optional[AbstractDriver] = None, **kwargs):
        """Initializes logger with an optional database driver."""
        if not self._initialized:
            self.db_driver = db_driver or self._setup_default_driver()
            for key, value in kwargs.items():
                setattr(self, key, value)

            self._initialized = True
            logger.info("MLWorkflowLogger initialized with driver: %s", type(self.db_driver).__name__)

    def _nx_DiGraph(self) -> None:
        """_Sets up the Networkx Graph_
        """
        self.graph = nx.DiGraph()

    def _setup_default_driver(self) -> AbstractDriver:
        """Setup default MongoDB driver if no driver is provided."""
        db_config = DBConfig(
            database='ml_workflows',
            collection="logs",
            db_type=DBType.MONGO,
            host='localhost',
            port=27017,
            username='root',
            password='password',
        )
        return MongoDBDriver(db_config)
    
    def log_flow(self, flow: Flow) -> None:
        """Log flow object, pass to driver for model conversion."""
        try:
            self.db_driver.save_flow(flow)
            logger.info("Flow logged successfully.")
        except Exception as e:
            logger.error(f"Failed to log flow: {e}")

    def log_run(self, run_name: str, run_id: Optional[str]) -> None:
        """Log run object, pass to driver for model conversion."""
        try:
            run_data = Run(run_name, run_id)
            self.db_driver.save_run(run_data)
            logger.info("Run logged successfully.")
        except Exception as e:
            logger.error(f"Failed to log run: {e}")

    def log_step(self, flow_id: str, step_name: str, step_info: str, step_data: StepModel) -> None:
        """Log step information, pass flow_id, step_name, step_data to the driver."""
        try:
            self.db_driver.save_step(flow_id, step_name, step_info, step_data) # Pass step details directly
            logger.info("Step logged successfully.")
        except Exception as e:
            logger.error(f"Failed to log step: {e}")

    def log_flow_record(self, flow_record_model: FlowRecordModel) -> None:
        """Log flow record object, pass to driver for model conversion."""
        try:
            self.db_driver.save_flow_record(flow_record_model)
            logger.info("Flow record logged successfully.")
        except Exception as e:
            logger.error(f"Failed to log flow record: {e}")

    def log_params(self, run_id: str, params: Dict[str, Any]) -> None:
        """Log parameters associated with a run."""
        try:
            self.db_driver.save_params(run_id, params)
            logger.info("Parameters logged successfully for run_id: %s", run_id)
        except Exception as e:
            logger.error(f"Failed to log parameters for run_id {run_id}: {e}")

    def log_metrics(self, run_id: str, metrics: Dict[str, Any]) -> None:
        """Log metrics associated with a run."""
        try:
            self.db_driver.save_metrics(run_id, metrics)
            logger.info("Metrics logged successfully for run_id: %s", run_id)
        except Exception as e:
            logger.error(f"Failed to log metrics for run_id {run_id}: {e}")

    def save_dataframe(self, run_id: str, df: pd.DataFrame) -> None:
        """Save the Dataframe associated with the run."""
        try:
            df.to_csv(f"{run_id}_data.csv", index=False)
            logger.info(f"Dataframe for run {run_id} saved successfully.")
        except Exception as e:
            logger.error(f"Failed to save DataFrame for run_id {run_id}: {e}")

    def end_run(self, run_id: str) -> None:
        """Mark the end of a run."""
        try:
            logger.info(f"Run {run_id} ended successfully.")
        except Exception as e:
            logger.error(f"Failed to end run for run_id {run_id}: {e}")