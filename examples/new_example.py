import logging
import os
from typing import Any, Dict

from ml_workflow_logger.drivers.mongodb import MongoDBDriver
from ml_workflow_logger.logger import MLWorkFlowLogger
from ml_workflow_logger.drivers.abstract_driver import DBConfig, DBType

# Set up Python's built-in logging for error handling
logging.basicConfig(level=logging.INFO)
logger_error = logging.getLogger(__name__)

# Sample logger configuration
config = DBConfig(
    db_type=DBType.MONGO,
    host="localhost",
    port=27017,
    database="ml_workflows",
    collection="logs",
    username="root",
    password="password",
)

log_dir = "logs"  # You can change this path as needed

# Initialize the MongoDBDriver
db_driver = MongoDBDriver(config)

# Ensure the log directory exists
os.makedirs(log_dir, exist_ok=True)

# Create the logger instance with the MongoDBDriver and log_dir
logger = MLWorkFlowLogger(db_driver_config=config)

# Create a flow
flow1_id = logger.add_new_flow("Flow1")

# Add steps to the flow
logger.add_new_step(flow1_id, "Step1", {"output": "output1"})
logger.add_new_step(flow1_id, "Step2", {"output": "output2"})
logger.add_new_step(flow1_id, "Step3", {"output": "output3"})

# Create multiple runs in the same process
for i in range(3):
    # Create a run
    run_id = logger.start_new_run(flow1_id)

    # link steps to run and flow
    logger.save_flow_record(flow1_id, run_id, "Step1", {"output": "output1"})
    logger.save_flow_record(flow1_id, run_id, "Step2", {"output": "output2"})
    logger.save_flow_record(flow1_id, run_id, "Step3", {"output": "output3"})

    # Add metrics to the run
    metrics = {"accuracy": 0.95, "loss": 0.05}
    logger.log_metrics(flow1_id, run_id, metrics)

    # End the run
    logger.end_run(flow1_id, run_id)

# End the flow
logger.end_flow(flow1_id)
