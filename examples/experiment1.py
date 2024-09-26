import threading
import pandas as pd
import uuid
import logging
import os
from ml_workflow_logger.drivers import mongodb
from ml_workflow_logger.logger import MLWorkFlowLogger
from ml_workflow_logger.drivers.abstract_driver import DBConfig, DBType
from ml_workflow_logger.drivers.mongodb import MongoDBDriver

# Set up Python's built-in logging for error handling
logging.basicConfig(level=logging.INFO)
logger_error = logging.getLogger(__name__)

# Sample logger configuration
config = DBConfig(
    db_type=DBType.MONGO,
    host='localhost',
    port=27017,
    database='ml_workflows',
    collection='logs',
    username='root',
    password='password'
)

# Set the directory for local log storage
log_dir = 'examples/logs'  # You can change this path as needed

# Initialize the MongoDBDriver
db_driver = MongoDBDriver(config)

# Ensure the log directory exists
os.makedirs(log_dir, exist_ok=True)

# Create the logger instance with the MongoDBDriver and log_dir
logger = MLWorkFlowLogger(db_driver=db_driver)

# Thread-safe wrapper for the logger
class ThreadSafeLogger:
    def __init__(self, logger: MLWorkFlowLogger) -> None:
        self.logger = logger
        self.lock = threading.Lock()

    def log_run(self, run_name: str, run_id: str) -> None:
        # Ensure run_id is a valid UUID
        if run_id is None:
            run_id = str(uuid.uuid4())
        with self.lock:
            self.logger.start_new_run(run_name, run_id)

    def log_metrics(self, run_id: str, metrics: dict):
        with self.lock:
            self.logger.log_metrics(run_id, metrics)

    def save_dataframe(self, run_id: str, df: pd.DataFrame):
        if not df.empty:
            with self.lock:
                self.logger.save_dataframe(run_id, df)
        else:
            logger_error.error("Invalid data format for saving DataFrame. Expected a non-empty DataFrame.")

    def end_run(self, run_id: str):
        with self.lock:
            self.logger.end_run(run_id)

# Initialize the thread-safe logger
thread_safe_logger = ThreadSafeLogger(logger)

# Start a new run
run_name = 'experiment_1'
run_id = str(uuid.uuid4())  # Use UUID for unique run identification

# Log a run using the thread-safe logger
thread_safe_logger.log_run(run_name, run_id)

# Log metrics
metrics = {'accuracy': 0.95, 'loss': 0.05}
thread_safe_logger.log_metrics(run_id, metrics)

# Generate and save a benchmark dataframe
data = {
    'epoch': [1, 2, 3],
    'accuracy': [0.8, 0.85, 0.9],
    'loss': [0.3, 0.25, 0.2]
}

# Convert data to a DataFrame outside the logger
df = pd.DataFrame(data)

# Save the DataFrame using the thread-safe logger
thread_safe_logger.save_dataframe(run_id, df)

# End the run
thread_safe_logger.end_run(run_id)

# Verify that the logger works as a singleton
another_logger = MLWorkFlowLogger()
print(logger is another_logger)  # Should print True if singleton is working
