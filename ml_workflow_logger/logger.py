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

    def add_new_flow(self, flow_name: str) -> str:
        """Log flow object, pass to driver for model conversion.

        Args:
            flow_name (str): Name of flow stored in logs
        """
        flow = Flow(flow_name)

        if self.local_mode:
            self._flows[flow.flow_id] = flow  # Save locally in memory
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
            self.save_dataframe(flow.flow_id, pd.DataFrame(flow.to_dict(), index=[flow.flow_id]))
        except Exception as e:
            logger.error(f"Failed to save flow '{flow.flow_id}' to CSV: {e}")

        return flow.flow_id

    def add_new_step(self, flow_id: str, step_name: str, step_data: Dict[str, Any] = {}) -> None:
        """Log step information, pass flow_id, step_name, step_data to the driver.

        Args:
            flow_id (str): Id given to every flow created
            step_name (str): Name of each step in the workflow
            step_data (Dict[str, Any], optional): Data captured in every step. Defaults to {}.
        """
        if self.local_mode:
            flow = self._flows.get(flow_id)
            if not flow:
                logger.error("Flow '%' does not exist.", flow_id)
                raise KeyError(f"Flow '{flow_id}' does not exist.")
            flow.add_step(step_name, step_data)
        else:
            if self.db_driver is None:
                logger.error("Database driver is not initialized.")
                raise AttributeError("Database driver is not initialized.")
            try:
                self.db_driver.add_step(flow_id, step_name, step_data)  # Save to database
                logger.info("Step '%s' logged successfully.", step_name)
            except Exception as e:
                logger.error(f"Failed to log step: '{step_name}' under Flow '{flow_id}': {e}")

        # Save step locally to CSV
        try:
            step_df = pd.DataFrame({"flow_id": [flow_id], "step_name": [step_name], "step_data": [step_data]}, index=[step_name])
            self.save_dataframe(flow_id, step_df)
        except Exception as e:
            logger.error(f"Failed to save step '{step_name}' to CSV: {e}")

    def start_new_run(self, run_id: Optional[str], flow_id: str) -> str:
        """Log run object, pass to driver for model conversion.

        Args:
            run_id (Optional[str]): A unique id created for each run

        Returns:
            str: The unique run_id for the run
        """

        if flow_id is None:
            logger.error("Flow ID is required to start a new run.")
            raise ValueError("Flow ID is required to start a new run.")

        if run_id is None:
            run_id = str(uuid.uuid4())
            logger.debug("Generated new run_id: %s", run_id)
        else:
            logger.debug("Using provided run_id: %s", run_id)

        run_data = Run(run_id, flow_id)

        if self.local_mode:
            flow = self._flows.get(flow_id)
            if flow is None:
                logger.error("Flow ID '%s' does not exist.", flow_id)
                raise KeyError(f"Flow ID '{flow_id}' does not exist.")
            else:
                run_data.flow_ref = flow_id
                with self._lock:
                    self._runs[run_id] = run_data  # Save run locally in memory
                    self._flows[flow_id].update_status("running")
                    logger.debug("Added run_id '%s' to in-memory runs.", run_id)
        else:
            if self.db_driver is None:
                logger.error("Database driver is not initialized.")
                raise AttributeError("Database driver is not initialized.")
            try:
                self.db_driver.save_new_run(run_data)  # Save to database
                logger.info("Run ID '%s' logged successfully.", run_id)
                self.db_driver.update_flow_status(flow_id, "Running")  # Update flow status to running
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
            logger.error(f"Failed to save metrics for run_id '{run_id}' to CSV: {e}")

    def save_flow_record(self, flow_id: str, run_id: str, step_name: str, step_data: Dict[str, Any] = {}) -> None:
        """Save the flow record for a particular step."""

        # run = self._runs.get(run_id)
        # if run is None:
        #     logger.error("Run ID '%s' does not exist.", run_id)
        #     raise KeyError(f"Run ID '{run_id}' does not exist.")
        
        # flow = self._flows.get(flow_id)
        # if flow is None:
        #     logger.error("Flow ID '%s' does not exist.", flow_id)
        #     raise KeyError(f"Flow ID '{flow_id}' does not exist.")
        
        # TODO: Implement saving to local storage
        
        if not self.local_mode:
            if self.db_driver is None:
                logger.error("Database driver is not initialized.")
                raise AttributeError("Database driver is not initialized.")
            try:
                self.db_driver.save_flow_record(flow_id, run_id, step_name, step_data)  # Save to database
                logger.info("Flow record for step '%s' logged successfully.", step_name)
                self.db_driver.update_run_status(run_id, "Running")  # Update run status to running
            except Exception as e:
                logger.error(f"Failed to log flow record for step '{step_name}': {e}")

        # Save flow record locally to CSV
        try:
            step_df = pd.DataFrame({"run_id": [run_id], "flow_id": [flow_id], "step_name": [step_name], "step_data": [step_data]}, index=[step_name])
            self.save_dataframe(run_id, step_df)
        except Exception as e:
            logger.error(f"Failed to save flow record for step '{step_name}' to CSV: {e}")


    def end_run(self, run_id: str) -> None:
        """Mark the end of a run.

        Args:
            run_id (str): To track the end of run
        """
        if self.local_mode:
            run = self._runs.get(run_id)
            if run is None:
                logger.error("Run ID '%s' does not exist.", run_id)
                raise KeyError(f"Run ID '{run_id}' does not exist.")
            
            run.end_run()
        else:
            if self.db_driver is None:
                logger.error("Database driver is not initialized.")
                raise AttributeError("Database driver is not initialized.")
            try:
                self.db_driver.update_run_status(run_id, "completed")  # Update run status to completed
                logger.info("Run ID '%s' marked as completed.", run_id)
            except Exception as e:
                logger.error(f"Failed to update run status for run_id '{run_id}': {e}")

        # Save run end status locally to CSV
        try:
            end_run_df = pd.DataFrame({"run_id": [run_id], "status": ["completed"]})
            self.save_dataframe(run_id, end_run_df)
        except Exception as e:
            logger.error(f"Failed to save end run status for run_id '{run_id}' to CSV: {e}")

    def end_flow(self, flow_id: str) -> None:
        """Mark the end of the flow and set the end time."""
        
        if self.local_mode:
            flow = self._flows.get(flow_id)
            if flow is None:
                logger.error("Flow ID '%s' does not exist.", flow_id)
                raise KeyError(f"Flow ID '{flow_id}' does not exist.")
            
            flow.update_status("completed")
        else:
            if self.db_driver is None:
                logger.error("Database driver is not initialized.")
                raise AttributeError("Database driver is not initialized.")
            try:
                self.db_driver.update_flow_status(flow_id, "completed")  # Update flow status to completed
                logger.info("Flow ID '%s' marked as completed.", flow_id)
            except Exception as e:
                logger.error(f"Failed to update flow status for flow_id '{flow_id}': {e}")

        # Save flow end status locally to CSV
        try:
            end_flow_df = pd.DataFrame({"flow_id": [flow_id], "status": ["completed"]})
            self.save_dataframe(flow_id, end_flow_df)
        except Exception as e:
            logger.error(f"Failed to save end flow status for flow_id '{flow_id}' to CSV: {e}")

    def save_dataframe(self, run_id: str, df: pd.DataFrame) -> None:
        """Save the Dataframe associated with the run.

        Args:
            run_id (str): Run_id recorded of current run.
            df (pd.DataFrame): Saves all the logged data in dataframe.
        """
        try:
            if not self.local_mode and self.db_driver:
                self.db_driver.save_dataframe(run_id, df)  # Save to database

            file_path = self.log_dir / f"{run_id}_data.csv"
            if file_path.exists():
                df.to_csv(file_path, mode='a', header=False, index=False)
            else:
                df.to_csv(file_path, mode='w', header=True, index=False)
            logger.info(f"Dataframe for run {run_id} saved successfully to {file_path}.")
        except Exception as e:
            logger.error(f"Failed to save DataFrame for run_id {run_id}: {e}")

