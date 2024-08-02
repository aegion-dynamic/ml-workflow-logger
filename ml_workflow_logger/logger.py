import os
import json
import threading
from typing import Any, Dict, Optional
import pandas as pd
import networkx as nx
from datetime import datetime
from pymongo import MongoClient
from pathlib import Path

from ml_workflow_logger.db_config import DBConfig, DBType

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
        Path(self.log_dir).mkdir(parents=True, exist_ok=True)
        self.current_run_dir = log_dir

        # Initialize Networkx graph
        self.graph = graph if graph is not None else nx.DiGraph()   

        # Initialize Database properties 
        self.db_config: DBConfig | None = db_config                                
        self.local_mode = db_config is None

        # Storage for database connection info
        self.client = None
        self.db = None
        self.collection = None

        if not self.local_mode:
            # Initialize Database
            self._setup_database()


    def _setup_database(self) -> None:
        """Sets up the database connection

        Raises:
            ValueError: If DBConfig is not provided for non-local mode
        """
        if self.local_mode is True:
            raise ValueError("DBConfig is required for non-local mode")
        
        if self.db_config  == 'mongodb':
            self.client = MongoClient(self.db_config.uri)
            self.db = self.client.get_database(self.db_config.database)
            self.collection = self.db.get_collection(self.db_config.collection)


    def start_run(self, run_name:str) -> None:
        """Starts a new run and creates a new directory for the run

        Args:
            run_name (_type_, optional): _description_. Defaults to None.
        """
        timestamp = datetime.now(). strftime('%Y%M%D-%H%M%S')
        run_name = run_name or f'run_{timestamp}'
        self.current_run_dir: Path = Path(self.log_dir) / run_name
        Path(self.current_run_dir).mkdir(parents=True, exist_ok=True)
        self.log_params({})
        self.log_metrics({})
        print(f"Started logging for {run_name}")

    
    def log_params(self, params: Dict[str, Any]) -> None:
        """Takes parameters like learning-rate, batch-size, epochs and adds them in params.json file

        Args:
            params (Dict[str, Any]): Parameters to be logged
        """
        self._log_data('params.json', params)

    
    def log_metrics(self, metrics: Dict[str, Any]) -> None:
        """Logs the performance metrics like accuracy and loss in metrics
        "It calculates the performance using metrics like accuracy and loss in and creates a metrics.json file"

        Args:
            metrics ( Dict[str, Any]): Metrics to be logged
        """
        self._log_data('metrics.json', metrics)

    
    def _log_data(self, filename: str, data: Dict[str, Any]) -> None:
        if self.local_mode:
            full_file_path = Path(self.current_run_dir) / filename
            with full_file_path.open('w') as f:
                json.dump(data, f)
        else:
            if self.db_config is None:
                raise ValueError("DBConfig is required for non-local mode")

            if self.db_config.db_type == DBType.MONGO:
                # TODO: Figure out the naming scheme for the collection
                # collection = self.db[filename.split('.')[0]]
                # collection.insert_one(data)
                # self.db.put_item(Item=data)
                pass


    def end_run(self) -> None:
        """Ends the current run and resets the current_run_dir
        """
        print(f"Finished logging for run {self.current_run_dir}")
        
        # TODO: Figure out what happens when we finish a run
        # self.current_run_dir = None

    
    def save_dataframe(self, df: pd.DataFrame) -> None:
        save_file_path = self.current_run_dir.joinpath('benchmark.csv')
        
        df.to_csv(path_or_buf=save_file_path,index=False)
        
        print(f"Generated benchmark CSV: {save_file_path}")


