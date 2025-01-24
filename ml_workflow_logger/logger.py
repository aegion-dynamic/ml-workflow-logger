import logging
import threading
import uuid
from pathlib import Path
from typing import Any, Dict, Optional
import pandas as pd

from ml_workflow_logger.drivers.abstract_driver import AbstractDriver, DBConfig, DBType
from ml_workflow_logger.drivers.mongodb import MongoDBDriver
from ml_workflow_logger.flow import Flow, Step
from ml_workflow_logger.local_data_store import LocalDataStore
from ml_workflow_logger.models import flow_record_model
from ml_workflow_logger.run import Run
from tests.test_drivers import db_config

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MLWorkFlowLogger:
    _instance = None
    _lock = threading.Lock()
    _is_local_mode = True

    def __new__(cls, *args, **kwargs) -> Any:
        """Ensure Singleton instance creation"""
        if cls._instance is None:
            with cls._lock:  # Ensure thread safety
                if cls._instance is None:
                    cls._instance = super(MLWorkFlowLogger, cls).__new__(cls)
        return cls._instance

    def __init__(self, log_dir: Path = Path('logs'), db_driver_config: Optional[DBConfig] = None, **kwargs):
        """Initialize the ML workflow Logger.

        Notes:
        Default DB driver connects to a local MongoDB instance. With the credentials: root:password

        Args:
            log_dir (str): The directory where logs are stored.
            graph (nx.DiGraph): Workflow graph visualization
            db_driver (Optional[MongoDBDriver], optional): The Database driver for logging to database, optionally creates a mongodb driver connection to localhost
        """
        with self._lock:
            if getattr(self, "_initialized", False):
                # If already initialized, optionally allow updating configuration
                if db_driver_config and self.local_mode:
                    # Transition from local to global mode
                    self.db_driver_config = db_driver_config
                    self.db_driver = self._setup_driver(db_driver_config)
                    self.local_mode = False
                    logger.info("Transitioned MLWorkFLowLogger to global mode with driver: %s", type(self.db_driver).__name__)
                return

            # Initialize Logs
            self.log_dir = log_dir
            # Ensure that the log directory exists
            Path(self.log_dir).mkdir(parents=True, exist_ok=True)

            # Initialize Database properties
            self.db_driver_config: DBConfig | None = db_driver_config
            self.local_mode = db_driver_config is None

            # Storage for the database connection info
            self.db_driver = None
            self.local_store = LocalDataStore()
            self._runs: Dict[str, Run] = {}
            self._flows: Dict[str, Flow] = {}


            if not self.local_mode:
                # Initialize Database
                self.db_driver= self._setup_driver(db_driver_config)
            self._initialized = True
            logger.info("MLWorkflowLogger initialized with driver: %s", type(self.db_driver).__name__)

        # if not self._initialized:

        #     # First check if its local mode or cloud mode
        #     if db_driver_config is None:
        #         # Local mode setup
        #         self.log_dir: Path = log_dir

        #         # These are the in-memory dictionaries to store the runs and flows
        #         self._runs: Dict[str, Run] = {}
        #         self._flows: Dict[str, Flow] = {}

        #     else:
        #         # Cloud mode setup
        #         self._is_local_mode = False
        #         self.db_driver = self._setup_driver(db_driver_config)


        #     for key, value in kwargs.items():
        #         setattr(self, key, value)

        #     self._initialized = True
        #     logger.info("MLWorkflowLogger initialized with driver: %s", type(self.db_driver).__name__)


    def _setup_driver(self, db_config) -> MongoDBDriver:
        """Setup default MongoDB driver if no driver is provided.

        Returns:
            AbstractDriver: _description_
        """
        # TODO: In the future, we can add support for other database drivers
        return MongoDBDriver(db_config)

    def add_new_flow(self, flow_name: str, flow_data: Dict[str, Any] = {}) -> None:
        """Log flow object, pass to driver for model conversion.

        Args:
            flow_name (str): Name of flow stored in logs
            run_id (str): Run id used to track the flow
            flow_data (Dict[str, Any], optional): All the flow data to be stored in the logs. Defaults to {}.
        """

        flow = Flow(flow_name, flow_data)

        if self.local_mode:
            self._flows[flow.flow_name] = flow  # Save locally in memory
        else:
            if self.db_driver is None:
                logger.error("Database driver is not initialized.")
                raise AttributeError("Database driver is not initialized.")
            try:
                self.db_driver.save_flow(flow)  # Save to database
                logger.info("Flow logged successfully.")
            except Exception as e:
                logger.error(f"Failed to log flow: {e}")

        # Save flow locally to CSV (whether in local or global mode)
        try:
            self.save_dataframe(flow.flow_name, pd.DataFrame(flow_data))
        except Exception as e:
            logger.error(f"Failed to log flow '{flow_name}': {e}")


    def add_new_step(self, flow_name: str, step_name: str, step_data: Dict[str, Any] = {}) -> None:
        """Log step information, pass flow_id, step_name, step_data to the driver.

        Args:
            flow_id (str): Id given to every flow created
            step_name (str): Name of each step in the workflow
            step_data (Dict[str, Any], optional): Data captured in every step. Defaults to {}.
        """
        if self.local_mode:
            flow = self._flows.get(flow_name)
            if not flow:
                logger.error("Flow '%' does not exist.", flow_name)
                raise KeyError(f"Flow '{flow_name}' does not exist.")
            flow.add_step(step_name, step_data)
        else:
            if self.db_driver is None:
                logger.error("Database driver is not initialized.")
                raise AttributeError("Database driver is not initialized.")
            try:
                self.db_driver.save_step(step_name, step_data)  # Save to database
                logger.info("Step '%s' logged successfully.", step_name)
            except Exception as e:
                logger.error(f"Failed to log step: '{step_name}' under Flow '{flow_name}': {e}")

        # Save step locally to CSV
        try:
            step_df = pd.DataFrame(step_data, index=[step_name])
            self.save_dataframe(flow_name, step_df)
        except Exception as e:
            logger.error(f"Failed to save step '{step_name}' to CSV: {e}")

    def start_new_run(self, run_id: Optional[str]) -> str:
        """Log run object, pass to driver for model conversion.

        Args:
            run_id (Optional[str]): A unique id created for each run

        Returns:
            str: The unique run_id for the run
        """
        if run_id is None:
            run_id = str(uuid.uuid4())
            logger.debug("Generated new run_id: %s", run_id)
        else:
            logger.debug("Using provided run_id: %s", run_id)

        run_data = Run(run_id)

        if self.local_mode:
            with self._lock:
                self._runs[run_id] = run_data  # Save run locally in memory
                logger.debug("Added run_id '%s' to in-memory runs.", run_id)
        else:
            if self.db_driver is None:
                logger.error("Database driver is not initialized.")
                raise AttributeError("Database driver is not initialized.")
            try:
                self.db_driver.save_new_run(run_data)  # Save to database
                logger.info("Run ID '%s' logged successfully.", run_id)
            except Exception as e:
                logger.error(f"Failed to log run '{run_id}': {e}")
                # If using in-memory storage clean up
                with self._lock:
                    if run_id in self._runs:
                        del self._runs[run_id]
                raise

        # Save run locally to CSV
        try:
            run_df = pd.DataFrame({"run_id": [run_id], "status": ["started"]})
            self.save_dataframe(run_id, run_df)
        except Exception as e:
            logger.error(f"Failed to save run '{run_id}' to CSV: {e}")

        return run_id

    def log_metrics(self, run_id: str, metrics: Dict[str, Any] = {}) -> None:
        """Log metrics associated with a run.

        Args:
            run_id (str): Run id used to track the metrics
            metrics (Dict[str, Any], optional): All the metrics used to measure the accuracy. Defaults to {}.
        """
        #TODO: Figure out what to do in the local version of the logger (save to run)

        if not self.local_mode:
            if self.db_driver is None:
                logger.error("Database driver is not initialized.")
                raise AttributeError("Database driver is not initialized.")
            try:
                self.db_driver.save_metrics(run_id, metrics)  # Save to database
                logger.info("Metrics logged successfully for run_id: %s", run_id)
            except Exception as e:
                logger.error(f"Failed to log metrics for run_id {run_id}: {e}")

        # Save metrics locally to CSV
        try:
            metrics_df = pd.DataFrame(metrics, index=[run_id])
            self.save_dataframe(run_id, metrics_df)
        except Exception as e:
            logger.error(f"Failed to save flow record for step '{run_id}' to CSV: {e}")

    def save_flow_record(self, run_id: str, step_name: str, step_data: Dict[str, Any] = {}) -> None:
        """Log flow record object, pass to driver for model conversion.

        Args:
            run_id (str): Used to track the flow_record with current run
            step_name (str): Used to identify appropriate record
            step_data (Dict[str, Any], optional): All the step data used to record step . Defaults to {}.
        """
        # TODO: Figure out the local version
        if not self.local_mode:
            if self.db_driver is None:
                logger.error("Database driver is not initialized.")
                raise AttributeError("Database driver is not initialized.")
            try:
                self.db_driver.save_flow_record(run_id, step_name, step_data)  # Save to database
                logger.info("Flow record logged successfully.")
            except Exception as e:
                logger.error(f"Failed to log flow record: {e}")

        # Save flow record locally to CSV
        try:
            record_df = pd.DataFrame(step_data, index=[step_name])
            self.save_dataframe(run_id, record_df)
        except Exception as e:
            logger.error(f"Failed to save flow record for step '{step_name}' under run_id '{run_id}' to CSV: {e}")

    def end_run(self, run_id: str) -> None:
        """Mark the end of a run.

        Args:
            run_id (str): To track the end of run
        """

        if self.local_mode:
            with self._lock:
                run = self._runs.get(run_id)
                if run:
                    run.status = "completed"
                else:
                    logger.error("Run ID '%s' does not exist.", run_id)
                    raise KeyError(f"Run ID '{run_id}' does not exist.")
        else:
            if self.db_driver is None:
                logger.error("Database driver is not initialized.")
                raise AttributeError("Database driver is not initialized")
            try:
                self.db_driver.update_run_status(run_id, "Completed")   # Save to database
                logger.info(f"Run {run_id} ended successfully in database.")
            except Exception as e:
                logger.error(f"Failed to end run for run_id {run_id}: {e}")

        # Save the final run status to CSV
        try:
            end_run_df = pd.DataFrame({"run_id": [run_id], "status": ["completed"]})
            self.save_dataframe(run_id, end_run_df)    
        except Exception as e:
            logger.error(f"Failed to end run for run_id '{run_id}' in database: {e}")

    def save_dataframe(self, run_id: str, df: pd.DataFrame) -> None:
        """Save the Dataframe associated with the run.

        Args:
            run_id (str): Run_id recorded of current run.
            df (pd.DataFrame): Saves all the logged data in dataframe.
        """
        try:
            file_path = self.log_dir / f"{run_id}_data.csv"
            if file_path.exists():
                df.to_csv(file_path, mode='a', header=False, index=False)
            else:
                df.to_csv(file_path, mode='w', header=True, index=False)
            logger.info(f"Dataframe for run {run_id} saved successfully to {file_path}.")
        except Exception as e:
            logger.error(f"Failed to save DataFrame for run_id {run_id}: {e}")

