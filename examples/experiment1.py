# Sample Usage:
from ml_workflow_logger.logger import MLWorkFlowLogger
from ml_workflow_logger.db_config import DBConfig, DBType
import pandas as pd

if __name__ == "__main__":
    config = DBConfig(
        db_type=DBType.MONGO,
        host="localhost",
        port=27017,
        database="ml_workflows",
        collection="logs",
        username="root",
        password="password"
    )

    logger = MLWorkFlowLogger(db_config=config)

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
# print(f"data being sent {data}")
# df = pd.DataFrame(data)
logger.save_dataframe(data)


# End the run
logger.end_run()

# Verify singleton behavior
another_logger = MLWorkFlowLogger()

# Output: True
print(logger is another_logger) 


# Adding Thread Safety
#class Thread(self):
 #   self.lock = threading.Lock()

# Wrap critical sections with lock
#with self.lock:

    # Critical section
 #   self._log_data('params.json', params)