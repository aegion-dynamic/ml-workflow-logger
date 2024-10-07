import threading
import logging
from typing import Any, Dict, Optional
#from venv import logger
from ml_workflow_logger.flow import Flow, Step
from ml_workflow_logger.run import Run
from ml_workflow_logger.drivers.mongodb import MongoDBDriver
from ml_workflow_logger.drivers.abstract_driver import AbstractDriver, DBConfig, DBType
import pandas as pd
from pathlib import Path
import uuid

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MLWorkFlowLogger:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs) -> Any:
        """Ensure Singleton instance creation"""
        if cls._instance is None:
            with cls._lock: # Ensure thread safety
                if cls._instance is None:
                    cls._instance = super(MLWorkFlowLogger, cls).__new__(cls)
        return cls._instance
    

    def __init__(self, log_dir: Path = Path('./'), db_driver: Optional[AbstractDriver] = None, **kwargs):
        """Initialize the ML workflow Logger.

        Notes:
        Default DB driver connects to a local MongoDB instance. With the credentials: root:password

        Args:
            log_dir (str): The directory where logs are stored.
            graph (nx.DiGraph): Workflow graph visualization
            db_driver (Optional[AbstractDriver], optional): The Database driver for logging to database, optionally creates a mongodb driver connection to localhost
        """
        if not getattr(self, '_initialized', False):
            self._initialized = False

        if not self._initialized:
            self.collection = log_dir
            self.db_driver = db_driver or self._setup_default_driver()

            self._runs: Dict[str, Run] = {}
            self._flows: Dict[str, Flow] = {}
            
            for key, value in kwargs.items():
                setattr(self, key, value)

            self._initialized = True
            logger.info("MLWorkflowLogger initialized with driver: %s", type(self.db_driver).__name__)


    def _setup_default_driver(self) -> AbstractDriver:
        """Setup default MongoDB driver if no driver is provided.

        Returns:
            AbstractDriver: _description_
        """
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
    
    def add_new_flow(self, flow_name: str, run_id: str, flow_data: Dict[str, Any] = {}) -> None:
        """Log flow object, pass to driver for model conversion.

        Args:
            flow_name (str): Name of flow stored in logs
            run_id (str): Run id used to track the flow
            flow_data (Dict[str, Any], optional): All the flow data to be stored in the logs. Defaults to {}.
        """

        flow = Flow(flow_name, run_id, flow_data)
        try:
            self.db_driver.save_flow(flow)
            logger.info("Flow logged successfully.")
        except Exception as e:
            logger.error(f"Failed to log flow: {e}")

    def add_new_step(self, flow_id: str, step_name: str, step_object: Step, step_data: Dict[str, Any] = {}) -> None:
        """Log step information, pass flow_id, step_name, step_data to the driver.

        Args:
            flow_id (str): Id given to every flow created
            step_name (str): Name of each step in the workflow
            step_data (Dict[str, Any], optional): Data captured in every step. Defaults to {}.
        """
        try:
            self.db_driver.save_step(step_name, step_data) # Pass step details directly
            logger.info("Step '%s' logged successfully.", step_name, flow_id)
        except Exception as e:
            logger.error(f"Failed to log step: '{step_name}' under Flow ID '{flow_id}': {e}")


    def start_new_run(self, run_name: str, run_id: Optional[str]) -> str:
        """Log run object, pass to driver for model conversion.

        Args:
            run_name (str): Run name given to each run created
            run_id (Optional[str]): A unique id created for each run

        Returns:
            str: The unique run_id for the run
        """
        if run_id is None:
            run_id = str(uuid.uuid4())
            logger.debug("Generated new run_id: %S", run_id)
        else:
            logger.debug("Using provided run_id: %s", run_id)

        run_data = Run(run_name, run_id)
        with self._lock:
            self._runs[run_id] = run_data
            logger.debug("Added run_id %s' to in-memory runs.", run_id)

        try:
            self.db_driver.save_new_run(run_data)
            logger.info("Run '%s' with Run ID '%s' logged successfully.", run_name, run_id)
        except Exception as e:
            logger.error(f"Failed to log run '{run_name}' with Run ID '{run_id}': {e}")
            with self._lock:
                del self._runs[run_id]  # Clean up the in-memory dictionary if DB logging fails
            raise  # Re-raise the exception after cleanup

        return run_id

    def log_metrics(self, run_id: str, metrics: Dict[str, Any] = {}) -> None:
        """Log metrics associated with a run.

        Args:
            run_id (str): Run id used to track the metrics
            metrics (Dict[str, Any], optional): All the metrics used to measure the accuracy. Defaults to {}.
        """
        
        try:
            self.db_driver.save_metrics(run_id, metrics)
            logger.info("Metrics logged successfully for run_id: %s", run_id)
        except Exception as e:
            logger.error(f"Failed to log metrics for run_id {run_id}: {e}")

    def save_flow_record(self, run_id:str, step_name: str, step_data: Dict[str, Any] = {}) -> None:
        """Log flow record object, pass to driver for model conversion.

        Args:
            run_id (str): Used to track the flow_record with current run
            step_name (str): Used to identify appropriate record
            step_data (Dict[str, Any], optional): All the step data used to record step . Defaults to {}.
        """
        try:
            self.db_driver.save_flow_record(run_id, step_name, step_data)
            logger.info("Flow record logged successfully.")
        except Exception as e:
            logger.error(f"Failed to log flow record: {e}")


    def end_run(self, run_id: str) -> None:
        """Mark the end of a run.

        Args:
            run_id (str): To track the end of run
        """
        with self._lock:
            run = self._runs.get(run_id)
            if run:
                run.status = "completed"
            else:
                logger.error("Run ID '%s' does not exist.", run_id)
                raise KeyError(f"Run ID '{run_id}' does not exist.")
        
        try:
            logger.info(f"Run {run_id} ended successfully.")
        except Exception as e:
            logger.error(f"Failed to end run for run_id {run_id}: {e}")


    def save_dataframe(self, run_id: str, df: pd.DataFrame) -> None:
        """Save the Dataframe associated with the run.

        Args:
            run_id (str): Run_id recorded of current run.
            df (pd.DataFrame): Saves all the logged data in dataframe.
        """
        try:
            #file_path =  self.log_dir / f"{run_id}_data.csv"
            df.to_csv(f"{run_id}_data.csv", index=False)
            logger.info(f"Dataframe for run {run_id} saved successfully.")
        except Exception as e:
            logger.error(f"Failed to save DataFrame for run_id {run_id}: {e}")
