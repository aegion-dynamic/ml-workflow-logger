import logging
import threading
from pathlib import Path
from typing import Any, Dict, Optional
import pandas as pd

from ml_workflow_logger.drivers.abstract_driver import DBConfig
from ml_workflow_logger.drivers.mongodb import MongoDBDriver
from ml_workflow_logger.flow import Flow, Step
from ml_workflow_logger.local_data_store import LocalDataStore
from ml_workflow_logger.models.flow_model import FlowModel
from ml_workflow_logger.models.run_model import RunModel
from ml_workflow_logger.models.flow_record_model import FlowRecordModel
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
        if self.local_mode:
            flow = Flow(flow_name)
            self._flows[flow.flow_id] = flow  # Save locally in memory
            try:
                self.save_dataframe(flow.flow_id, pd.DataFrame([flow.to_dict()], index=[flow.flow_id]))
            except Exception as e:
                logger.error(f"Failed to save the datagram for flow '{flow.flow_id}': {e}")
            return flow.flow_id
        else:
            flow = FlowModel(name=flow_name)
            flow_dict = flow.to_dict()
            if self.db_driver is None:
                logger.error("Database driver is not initialized.")
                raise AttributeError("Database driver is not initialized.")
            try:
                self.db_driver.save_flow(flow_dict)  # Save to database
                self.db_driver.save_dataframe(flow_dict["_id"], f"New flow is created with flow id {flow_dict['_id']}", pd.DataFrame([flow_dict], index=[flow_dict["_id"]]))
                logger.info("Flow logged successfully.")  
            except Exception as e:
                logger.error(f"Failed to log flow: {e}")
            return flow_dict["_id"]

    def add_new_step(self, flow_id: str, step_name: str, step_data: Dict[str, Any] = {}) -> None:
        """Add a new step to the flow.

        Args:
            flow_id (str): ID of the flow to which the step belongs.
            step_name (str): Name of the step.
            step_data (Dict[str, Any], optional): Data related to the step. Defaults to {}.
        """
        if self.local_mode:
            step = Step(step_name=step_name, step_data=step_data)
            self._flows[flow_id].add_step(step)
            try:
                step_df = pd.DataFrame([step.to_dict()], index=[step.step_id])
                self.save_dataframe(flow_id, step_df)
            except Exception as e:
                logger.error(f"Failed to save step '{step_name}' to CSV: {e}")
        else:
            if self.db_driver is None:
                logger.error("Database driver is not initialized.")
                raise AttributeError("Database driver is not initialized.")
            try:
                self.db_driver.add_step(flow_id, step_name, step_data)  # Save to database
                step_df = pd.DataFrame([step_data], index=[step_name])
                self.db_driver.save_dataframe(flow_id, f"New step is added to the flow with flow id {flow_id}", step_df)
                logger.info("Step '%s' logged successfully.", step_name)
            except Exception as e:
                logger.error(f"Failed to log step: '{step_name}' under Flow '{flow_id}': {e}")

    def start_new_run(self, flow_id: str) -> str:
        """Start a new run for a given flow.

        Args:
            flow_id (str): ID of the flow to start the run for.

        Returns:
            str: ID of the new run.
        """
        if self.local_mode:
            flow = self._flows.get(flow_id)
            if flow is None:
                logger.error("Flow ID '%s' does not exist.", flow_id)
                raise KeyError(f"Flow ID '{flow_id}' does not exist.")
            else:
                run = Run(flow_ref=flow_id)
                run_dict = run.to_dict()
                self._runs[run_dict["run_id"]] = run  # Save locally in memory
                flow.update_status("running")
                try:
                    run_df = pd.DataFrame({"run_id": [run_dict["run_id"]], "status": ["created"]})
                    self.save_dataframe(run_dict["run_id"], run_df)
                except Exception as e:
                    logger.error(f"Failed to save run {run_dict['run_id']} to CSV: {e}")
                return run_dict["run_id"]
        else:
            run = RunModel(flow_ref=flow_id)
            run_dict = run.to_dict()
            run_id = run_dict["_id"]
            if self.db_driver is None:
                logger.error("Database driver is not initialized.")
                raise AttributeError("Database driver is not initialized.")
            try:
                self.db_driver.save_new_run(run_dict)  # Save to database
                self.db_driver.save_dataframe(flow_id, f"New run is started with run id {run_id} to the flow with flow id {flow_id}", pd.DataFrame([run_dict], index=[run_id]), run_id)
                logger.info("Run ID '%s' logged successfully.", run_id)
            except Exception as e:
                logger.error(f"Failed to log run: '{run_id}' under Flow '{flow_id}': {e}")
            return run_id

    def log_metrics(self, flow_id: str, run_id: str, metrics: Dict[str, Any] = {}) -> None:
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
                metrics_df = pd.DataFrame(metrics, index=[run_id])
                self.db_driver.save_dataframe(flow_id, f"Metrics of run with run id {run_id} updated", metrics_df, run_id)
            except Exception as e:
                logger.error(f"Failed to log metrics for run_id {run_id}: {e}")

        # # Save metrics locally to CSV
        # try:
        #     metrics_df = pd.DataFrame(metrics, index=[run_id])
        #     self.save_dataframe(run_id, metrics_df)
        # except Exception as e:
        #     logger.error(f"Failed to save metrics for run_id '{run_id}' to CSV: {e}")

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
            flow_record = FlowRecordModel(step_name=step_name, step_data=step_data, flow_ref=flow_id, run_ref=run_id)
            flow_record_dict = flow_record.to_dict()
            if self.db_driver is None:
                logger.error("Database driver is not initialized.")
                raise AttributeError("Database driver is not initialized.")
            try:
                self.db_driver.save_flow_record(flow_record_dict)  # Save to database
                logger.info("Flow record for step '%s' logged successfully.", step_name)
                self.db_driver.update_run_status(run_id, "Running")  # Update run status to running
                step_df = pd.DataFrame({"run_id": [run_id], "flow_id": [flow_id], "step_name": [step_name], "step_data": [step_data]}, index=[step_name])
                self.db_driver.save_dataframe(flow_id, f"Step with step name {step_name} of the flow with flow id {flow_id} at run with run id {run_id}", step_df, run_id)
            except Exception as e:
                logger.error(f"Failed to log flow record for step '{step_name}': {e}")

        # # Save flow record locally to CSV
        # try:
        #     step_df = pd.DataFrame({"run_id": [run_id], "flow_id": [flow_id], "step_name": [step_name], "step_data": [step_data]}, index=[step_name])
        #     self.save_dataframe(run_id, step_df)
        # except Exception as e:
        #     logger.error(f"Failed to save flow record for step '{step_name}' to CSV: {e}")


    def end_run(self, flow_id: str, run_id: str) -> None:
        """End the current run.

        Args:
            run_id (str): ID of the run to end.
        """
        if self.local_mode:
            run = self._runs.get(run_id)
            if run is None:
                logger.error("Run ID '%s' does not exist.", run_id)
                raise KeyError(f"Run ID '{run_id}' does not exist.")
            run.update_status("completed")
            try:
                end_run_df = pd.DataFrame({"run_id": [run_id], "status": ["completed"]})
                self.save_dataframe(run_id, end_run_df)
            except Exception as e:
                logger.error(f"Failed to save run '{run_id}' to CSV: {e}")
        else:
            if self.db_driver is None:
                logger.error("Database driver is not initialized.")
                raise AttributeError("Database driver is not initialized.")
            try:
                self.db_driver.update_run_status(run_id, "completed")  # Update run status to completed
                end_run_df = pd.DataFrame({"run_id": [run_id], "status": ["completed"]})
                self.db_driver.save_dataframe(flow_id, f"Run is completed with run id {run_id} for the flow with flow id {flow_id}", end_run_df, run_id)
                logger.info("Run ID '%s' marked as completed.", run_id)
            except Exception as e:
                logger.error(f"Failed to update run status for run_id '{run_id}': {e}")


    def end_flow(self, flow_id: str) -> None:
        """Mark the end of the flow and set the end time.

        Args:
            flow_id (str): ID of the flow to end.
        """
        if self.local_mode:
            flow = self._flows.get(flow_id)
            if flow is None:
                logger.error("Flow ID '%s' does not exist.", flow_id)
                raise KeyError(f"Flow ID '{flow_id}' does not exist.")
            flow.update_status("completed")
            try:
                end_flow_df = pd.DataFrame({"flow_id": [flow_id], "status": ["completed"]})
                self.save_dataframe(flow_id, end_flow_df)
            except Exception as e:
                logger.error(f"Failed to save flow '{flow_id}' to CSV: {e}")
        else:
            if self.db_driver is None:
                logger.error("Database driver is not initialized.")
                raise AttributeError("Database driver is not initialized.")
            try:
                self.db_driver.update_flow_status(flow_id, "completed")  # Update flow status to completed
                end_flow_df = pd.DataFrame({"flow_id": [flow_id], "status": ["completed"]})
                self.db_driver.save_dataframe(flow_id, f"Flow is completed with flow id {flow_id}", end_flow_df)
                logger.info("Flow ID '%s' marked as completed.", flow_id)
            except Exception as e:
                logger.error(f"Failed to update flow status for flow_id '{flow_id}': {e}")

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

