import os
import json
import threading
import pandas as pd
import networkx as nx
from datetime import datetime
from pymongo import MongoClient

class MLWorkFlowLogger:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(MLWorkFlowLogger, cls).__new__(cls)
            cls._instance.__init__(*args, **kwargs)
        return cls._instance

    def __init__(self, log_dir='logs', graph=None, db_config=None):
        self.log_dir = log_dir                                       # Initialize Logs
        os.makedirs(self.log_dir, exist_ok=True)
        self.current_run_dir = None
        self.graph = graph if graph is not None else nx.DiGraph()    # Initialize Networkx graph
        self.db_config = db_config                                   # Initialize Database
        self.local_mode = db_config is None
        self.client = None
        self.db = None
        if not self.local_mode:
            self._setup_database()


    def _setup_database(self):
        if self.db_config['type'] == 'mongodb':
            self.client = MongoClient(self.db_config['uri'])
            self.db = self.client[self.db_config['database']]
            self.db = self.client.Table(self.db_config['table'])


    def start_run(self, run_name=None):
        timestamp = datetime.now(). strftime('%Y%M%D_%H%M%S')
        run_name = run_name or f'run_{timestamp}'
        self.current_run_dir = os.path.join(self.log_dir, run_name)
        os.makedirs(self.current_run_dir, exist_ok=True)
        self.log_params({})
        self.log_metrics({})
        print(f"Started logging for {run_name}")

    
    def log_params(self, params: dict[str, any]) -> None:
        "Takes parameters like learning-rate, batch-size, epochs and adds them in params.json file"
        self._log_data('params.json', params)
    print(log_params.__doc__)

    
    def log_metrics(self, metrics):
        "It calculates the performance using metrics like accuracy and loss in and creates a metrics.json file"
        self._log_data('metrics.json', metrics)
    print(log_metrics.__doc__)

    
    def _log_data(self, filename, data):
        if self.local_mode:
            with open(os.path.join(self.current_run_dir, filename), 'w') as f:
                json.dump(data, f)
        else:
            if self.db_config['type'] == 'mongodb':
                collection = self.db[filename.split('.')[0]]
                collection.insert_one(data)
                self.db.put_item(Item=data)


    def end_run(self):
        print(f"Finished logging for run {self.current_run_dir}")
        self.current_run_dir = None

    
    def generate_benchmark_df(self, data):
        df = pd.DataFrame(data)
        df.to_csv(os.path.join(self.current_run_dir, 'benchmark.csv'),index=False)
        print(f"Generated benchmark CSV: {os.path.join(self.current_run_dir, 'benchmark.csv')}")


# Sample Usage:
logger = MLWorkFlowLogger()


# Start a new run
logger.start_run('experiment_1')

# Log parameters
params = {
    'learning_rate': 0.01,
    'batch_size': 32,
    'num_epochs': 10
}
logger.log_params(params)

# Log metrics
metrics = {
    'accuracy': 0.95,
    'loss': 0.05
}
logger.log_metrics(metrics)

# Generate a benchmark dataframe
data = {
    'epoch': [1,2,3],
    'accuracy': [0.8, 0.85, 0.9],
    'loss': [0.3, 0.25, 0.2]
}
logger.generate_benchmark_df(data)

# End the run
logger.end_run()

# Verify singleton behavior
another_logger = MLWorkFlowLogger()
print(logger is another_logger) # Output: True


# Adding Thread Safety
#class Thread(self):
 #   self.lock = threading.Lock()

# Wrap critical sections with lock
#with self.lock:

    # Critical section
 #   self._log_data('params.json', params)
