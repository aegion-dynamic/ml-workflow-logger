from optparse import Option

from ml_workflow_logger.drivers.mongodb import MongoDBDriver
from ml_workflow_logger.flow import Flow
from typing import Any, Dict, Optional, Union
import pandas as pd
import networkx as nx
from pymongo import MongoClient
from pathlib import Path
from ml_workflow_logger.local_data_store import LocalDataStore
from ml_workflow_logger.run import Run
from ml_workflow_logger.models.flow_model import FlowModel
from ml_workflow_logger.models.flow_record_model import FlowRecordModel
from ml_workflow_logger.drivers.abstract_driver import AbstractDriver, DBConfig, DBType


class MLWorkFlowLogger:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(MLWorkFlowLogger, cls).__new__(cls)
            cls._instance.__init__(*args, **kwargs)
        return cls._instance
    

    def __init__(self, log_dir: Path=Path('logs'), db_config: Optional[DBConfig]=None):
        """Initializes the MLWorkFlowLogger class

        Args:
            log_dir (Path, optional): Directory where the logs are stored . Defaults to Path('logs').
            db_config (Optional[DBConfig], optional): Config for the database. Defaults to None.
        """
        if hasattr(self, '_initialized') and self._initialized:
            return

        # Initialize Logs
        self.log_dir = log_dir
        # Ensure that the log directory exists                                     
        Path(self.log_dir).mkdir(parents=True, exist_ok=True)
       
        # Initialize Database properties 
        self.db_config: DBConfig | None = db_config                                
        self.local_mode = db_config is None

        # Storage for database connection info
        self.db_driver: Optional[Union[MongoDBDriver, AbstractDriver]] = None
        self.local_store = LocalDataStore()

        # Create the run object
        self._current_run: Optional[Run] = None

        # Create the flow object
        self._current_flow: Optional[Flow] = None

        if not self.local_mode:
            # Initialize Database
            self._setup_global_database()
        self._initialized = True


    def _nx_DiGraph(self) -> None:
        """_Sets up the Networkx Graph_
        """
        self.graph = nx.DiGraph()


    def _setup_global_database(self) -> None:
        """Sets up the global database connection

        Raises:
            ValueError: If DBConfig is not provided for non-local mode
        """
        # First check if its local mode and if it is, then we don't need to setup the database
        if self.local_mode:
            raise ValueError("DBConfig is required for non-local mode")
        
        # Now based on what kind of database we use, we can setup the connection correctly
        if self.db_config is None:
            raise ValueError("DBConfig is required for non-local mode")
        
        if self.db_config.db_type == DBType.MONGO:
            self.db_driver = MongoDBDriver(self.db_config)

        elif self.db_config.db_type == DBType.SQLITE:
            raise NotImplementedError("SQLite is not implemented yet")
        elif self.db_config.db_type == DBType.POSTGRES:
            raise NotImplementedError("Postgres is not implemented yet")
        else:
            raise ValueError("Invalid DBType")
        
    
    def start_run(self, run_name: str) -> None:
        """Starts a new run and creates a new directory for the run

        Args:
            run_name (_type_, optional): _description_. Defaults to None.
        """
        self._current_run = Run(run_name=run_name)
        self._current_run.create_run_dir(self.log_dir)

        # Save the run to the local store
        # Manually create a dictionary representation of the run
        run_data = {
            'run_id': self._current_run.run_id,
            'run_name': self._current_run.run_name,
            'run_dir': str(self._current_run.run_dir),  # Convert PosixPath to string
            # Add other attributes as needed
        }
        
        self.local_store.save_run(self._current_run.run_id, run_data)

        if self.db_driver is not None:
            self.db_driver.save_run(self._current_run)


        print(f"Current run directory: {self._current_run.run_dir}")


    def add_flow(self, flow_name: str, flow_data: Optional[Dict[str, Any]] = None) -> None:
            """Adds a new flow to current run

            Args:
                flow_name (str): Name of the flow_
                flow_data (Optional[Dict[str, Any]], optional): Additional data related to the flow. Defaults to None.
            """
            if self._current_run is None:
                raise ValueError("No active run. Start a run before adding flows")
            
            # Create the new flow
            new_flow = Flow(flow_name=flow_name, run_id=self._current_run.run_id, flow_data=flow_data)

            # Add the flow to the current run
            self._current_flow = new_flow

            # Log the flow details to the local store
            flow_record =FlowRecordModel(
                step_name="init",
                step_data={"info": f"Flow {flow_name} initialized"},
                flow_ref=new_flow,
                run_ref=self._current_run
            )
            self.local_store.save_step(self._current_run.run_id, flow_record.to_dict())

            # Log to global database if available
            if self.db_driver is not None:
                self.db_driver.save_flow(new_flow)

            print(f"Added new flow: {flow_name}")


    def log_step(self, step_name: str, step_info: str) -> None:
        """Starts logging the steps after run is initiated

        Args:
            step_name (str): _description_. Defaults to None
            step_info (str): _description_. Defaults to None
        """
        if self._current_run is None:
            raise ValueError("No active run. Start a run before logging steps")
        
        if self._current_flow is None:
            raise ValueError("No active flow. Add a flow before logging steps")

        self._current_flow.add_step(step_name=step_name, step_info=step_info)

        flow_record = FlowRecordModel(
            step_name=step_name,
            step_data={"info": step_info},
            flow_ref=self._current_flow,
            run_ref=self._current_run
        )

        #Log to local store
        self.local_store.save_step(self._current_run.run_id, flow_record.to_dict())
        #Log to global database if available
        if self.db_driver is not None:
            self.db_driver.save_flow_record(flow_record)
        print(f"Logged step: {flow_record.step_name} - Data: {flow_record.step_data}")


    def log_params(self, params: Dict[str, Any]) -> None:
        """Logs parameters like learning-rate, batch-size, epochs etc.

        Args:
            params (Dict[str, Any]): Parameters to be logged
        """
        if self._current_run is None:
            raise ValueError("No active run. Start a run before logging parameters")

        # Insert each of the keys in the params into the local store
        for key, value in params.items():
            self._current_run.add_param(key, value)

        #log to local store
        self.local_store.save_params(self._current_run.run_id, params)
        #log to global database
        if self.db_driver is not None:
            self.db_driver.save_params(self._current_run.run_id, params)
        
        print(f"Logged parameters: {params}")
        
    
    def log_metrics(self, metrics: Dict[str, Any]) -> None:
        """Logs the performance metrics like accuracy and loss in metrics
        "It calculates the performance using metrics like accuracy and loss in and creates a metrics.json file"

        Args:
            metrics ( Dict[str, Any]): Metrics to be logged
        """
        if self._current_run is None:
            raise ValueError("No active run. Start a run before logging metrics")

        # Insert each of the keys in the metrics into the local store
        for key, value in metrics.items():
            self._current_run.add_metric(key, value)

        #Log to local store
        self.local_store.sav_metrics(self._current_run.run_id, metrics)
        
        #Log to global database if available
        if self.db_driver is not None:
            self.db_driver.save_metrics(self._current_run.run_id, metrics)
        print(f"Logged metrics: {metrics}")


    def end_run(self) -> None:
        """Ends the current run and resets the current_run_dir."""

        if self._current_run is None:
            raise ValueError("No active run. Start a run before ending it")


        print(f"Run ended with ID: {self._current_run.run_id}")
        # print(f"Finished logging for run {self._current_flow.run_name}")
        
        self._current_run = None
        self._current_flow = None
    

    def save_dataframe(self, df: Dict) -> None:
        """Saves the dataframe as a CSV file in the current run directory

        Args:
            df (Dict): Data to be saved
        """
        if self._current_run is None:
            raise ValueError("No active run. Start a run before saving dataframes")

        # Convert the dictionary to a DataFrame
        df_new = pd.DataFrame(df)

        # Save the DataFrame as a CSV locally
        save_file_path = self._current_run.run_dir.joinpath('benchmark.csv')
        print(f"Type of df{type(df_new)}")
        df_new.to_csv(path_or_buf=save_file_path,index=False)

        # Save the DataFrame to local store
        self.local_store.save_dataframe(self._current_run.run_id, df_new)

        # TODO - Detrmine if this is needed now
        # # Convert DataFrame to dictionary and save it to MongoDB
        # if self.db_driver is not None:
        #     # df_dict = df(orient="records")
        #     self.global_collection.update_one(
        #         {"run_id": self._current_run.run_id},
        #         {"$set": {"benchmark_csv": df}},
        #         upsert=True
        #     )
        print(f"Generated benchmark CSV: {save_file_path}")

        # print(f"DataFrame shape: {df.shape}")


