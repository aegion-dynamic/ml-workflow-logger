import sys
import os
sys.path.append('/home/venom983/Desktop/rp-ml-workflow-logger')
import json
from typing import Any, Dict, Optional
import pandas as pd
import networkx as nx
from datetime import datetime
from pymongo import MongoClient
from pydantic import BaseModel
from pathlib import Path
from ml_workflow_logger.local_data_store import LocalDataStore
from ml_workflow_logger.run import Run
from ml_workflow_logger.models.flow import Flow, Step
from ml_workflow_logger.models.flow_record import FlowRecord
from ml_workflow_logger.db_config import DBConfig, DBType, get_mongodb_collection


class MLWorkFlowLogger:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(MLWorkFlowLogger, cls).__new__(cls)
            cls._instance.__init__(*args, **kwargs)
        return cls._instance

    def __init__(self, log_dir: Path=Path('logs'), graph: Optional[nx.DiGraph]=None, db_config: Optional[DBConfig]=None):
        """Initializes the MLWorkFlowLogger class

        Args:
            log_dir (Path, optional): Directory where the logs are stored . Defaults to Path('logs').
            graph (Optional[nx.DiGraph], optional): Graph object to map the outputs against. Defaults to None.
            db_config (Optional[DBConfig], optional): Config for the database. Defaults to None.
        """

        # Initialize Logs
        self.log_dir = log_dir
        # Ensure that the log directory exists                                     
        Path(self.log_dir).mkdir(parents=True, exist_ok=True)

        # Initialize Networkx graph
        self.graph = graph if graph is not None else nx.DiGraph()   

        # Initialize Database properties 
        self.db_config: DBConfig | None = db_config                                
        self.local_mode = db_config is None

        # Storage for database connection info
        self.client = None
        self.db = None
        self.collection = None
        self.db_config: DBConfig | None = db_config
        self.local_mode = db_config is None

        # Local data store
        self.local_store = None

        # Create the run object
        self._current_run = None

        if not self.local_mode:
            # Initialize Database
            self._setup_database()
        else:
            # TODO: Initialize a local data store to store the step logs
            self.local_store = LocalDataStore()


    def _setup_database(self) -> None:
        """Sets up the database connection

        Raises:
            ValueError: If DBConfig is not provided for non-local mode
        """
        # First check if its local mode and if it is, then we don't need to setup the database
        if self.local_mode:
            raise ValueError("DBConfig is required for non-local mode")
        
        # Now based on what kind of database we use, we can setup the connection correctly
        if self.db_config.db_type == DBType.MONGO:
            self.client = MongoClient(self.db_config.uri)
            self.db = self.client.get_database(self.db_config.database)
            self.collection = self.db.get_collection(self.db_config.collection)
        elif self.db_config.db_type == DBType.SQLITE:
            raise NotImplementedError("SQLite is not implemented yet")
        elif self.db_config.db_type == DBType.POSTGRES:
            raise NotImplementedError("Postgres is not implemented yet")
        else:
            raise ValueError("Invalid DBType")
        

    def start_run(self, run_name:str) -> None:
        """Starts a new run and creates a new directory for the run

        Args:
            run_name (_type_, optional): _description_. Defaults to None.
        """
        self._current_run = Run(run_name)
        timestamp = datetime.now(). strftime('%Y%M%D-%H%M%S')
        run_name = run_name or f'run_{timestamp}'
        self._current_run.create_run_dir(self.log_dir)
        self._current_flow = Flow(name=run_name)
        
        print(f"Started logging for {run_name}")
        print(f"Current run directory: {self.current_run_dir}")


    def log_step(self, step_name: str, step_info: str) -> None:
        """Starts logging the steps after run is initiated

        Args:
            step_name (str): _description_. Defaults to None
            step_info (str): _description_. Defaults to None
        """
        step = Step(step_name=step_name, step_info=step_info)
        flow = Flow(name="current_flow")
        flow.add_step(step_name, step_info)

        flow_record = FlowRecord(
            step_name=step_name,
            step_data={"info: step_info"},
            flow_ref=flow,
            run_ref=self._current_run
        )
        print(f"step: {flow_record.step_name} - Data: {flow_record.step_data}")

        if self.db:
            self.collection.insert_one(flow_record.to_dict)


    def log_params(self, params: Dict[str, Any]) -> None:
        """Takes parameters like learning-rate, batch-size, epochs and adds them in params.json file

        Args:
            params (Dict[str, Any]): Parameters to be logged
        """
        # Insert each of the keys in the params into the local store
        for key, value in params.items():
            self._current_run.add_param(key, value)
        
        print(f"Logged parameters: {params}")
        
    
    def log_metrics(self, metrics: Dict[str, Any]) -> None:
        """Logs the performance metrics like accuracy and loss in metrics
        "It calculates the performance using metrics like accuracy and loss in and creates a metrics.json file"

        Args:
            metrics ( Dict[str, Any]): Metrics to be logged
        """
        for key, value in metrics.items():
            self._current_run.add_metric(key, value)
        
        print(f"Logged metrics: {metrics}")


    # def log_step(self, step: str, data: Dict[str, Any]) -> None:
    #     """Logs the data for a step in the workflow

    #     Args:
    #         data (Dict[str, Any]): Data to be logged
    #     """
    #     # TODO: Figure out how to save it
    #     # TODO: Check and see if step name is in the graph nodes
    #     self.logger.info


    def end_run(self) -> None:
        """Ends the current run and resets the current_run_dir
        """
        if self.db:
            self.collection.insert_one(self._current_run.dict())
        print(f"Run ended with ID: {self._current_run.run_id}")
        #self.run_id = None
        self._current_run = None
        print(f"Finished logging for run {self.current_flow.name}")
        #self._current_flow = None
        
        # TODO: Figure out what happens when we finish a run
        # self.current_run_dir = None

    
    def save_dataframe(self, df: pd.DataFrame) -> None:
        """Saves the dataframe as a CSV file in the current run directory

        Args:
            df (pd.DataFrame): DataFrame to be saved
        """
        save_file_path = self.current_run_dir.joinpath('benchmark.csv')
        
        df.to_csv(path_or_buf=save_file_path,index=False)
        
        print(f"Generated benchmark CSV: {save_file_path}")
        print(f"DataFrame shape: {df.shape}")


