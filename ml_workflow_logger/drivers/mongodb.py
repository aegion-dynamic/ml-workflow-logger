import logging
from typing import Any, Dict
from pymongo import MongoClient
from ml_workflow_logger.drivers.abstract_driver import AbstractDriver
from ml_workflow_logger.drivers.abstract_driver import DBConfig
from ml_workflow_logger.flow import Flow
from ml_workflow_logger.models.flow_model import FlowModel, StepModel
from ml_workflow_logger.models.flow_record_model import FlowRecordModel
from ml_workflow_logger.models.run_model import RunModel
import pandas as pd
from ml_workflow_logger.run import Run

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def _create_mongodb_client(config: DBConfig) -> MongoClient:
    """Create and return a MongoDB client based on the DB configuration."""
    return MongoClient(
        host=config.computed_connection_uri,
        username=config.username,
        password=config.password,
        port=config.port
    )

class MongoDBDriver(AbstractDriver):
    """MongoDB Driver implementation of AbstractDriver."""

    def __init__(self, db_config: DBConfig) -> None:
        """Initialize MongoDB client and database."""
        try:
            self._client = _create_mongodb_client(db_config)
            self._db = self._client[db_config.database]

            # Check if all the required collections are present
            collections_to_check = ['flow_models', 'run_models', 'flowrecord_models', 'step_models', 'dataframes']
            for collection in collections_to_check:
                if collection not in self._db.list_collection_names():
                    self._db.create_collection(collection)
                    logger.info("Created collection: {collection}")

            #Add index to important fields (e.g., run_id)
            self._db['run_models'].create_index('run_id', unique=True)
            logger.info("created index on run_id for runn_models collection")
        
        except Exception as e:
            logger.error(f"Error initializing MongoDB: {e}")
            raise
    
    def _convert_to_dict(self, data: Any) -> Dict[str, Any]:
        """Convert model instances to dictionaries for MongoDB."""
        return data.dict() if hasattr(data, 'dict') else data
    
    def _validate_data(self, data: Dict[str, Any]) -> bool:
        """Basic validation of data before saving to MongoDB."""
        if not isinstance(data, dict) or not data:
            logger.error("Invalid data format. Expected a non-empty dictionary.")
            return False
        return True
    
    def save_flow(self, flow_data: Flow) -> None:
        # Save the flow data to the flowmodels collection
        collection = self._db['flow_models']
        data = self._convert_to_dict(flow_data)
        if self._validate_data(data):     
            try:
                collection.insert_one(data)
                logger.info("Flow data saved successfully.")
            except Exception as e:
                logger.error(f"Error saving flow data: {e}")
    
    def save_run(self, run_data: Run) -> None:
        # Save the run data to the runmodels collection
        collection = self._db['run_models']
        data = self._convert_to_dict(run_data)
        if self._validate_data(data):
            try:
                collection.insert_one(data)
                logger.info("Run data saved successfully.")
            except Exception as e:
                logger.error(f"Error saving run data: {e}")

    def save_step(self, flow_id, step_name, step_info, step_data: StepModel) -> None:
        # Save the step data to the stepmodels collection
        collection = self._db['step_models']
        data = self._convert_to_dict(step_data)
        if self._validate_data(data):
            try:
                collection.insert_one(data)
                logger.info("Step data saved successfully.")
            except Exception as e:
                logger.error(f"Error saving the step data: {e}")

    def save_flow_record(self, flow_record_data: FlowRecordModel) -> None:
        # Save the flow record data to the flowrecord_models collection
        collection = self._db['flowrecord_models']
        data = self._convert_to_dict(flow_record_data)
        if self._validate_data(data):
            try:
                collection.insert_one(data)
                logger.info("Flow record data saved successfully.")
            except Exception as e:
                logger.error(f"Error saving flow record data: {e}")    

    def _update_run_data(self, run_id: str, data: Dict[str, Any], key: str) -> None:
        """Utility method to update run data (params/metrics)."""
        Collection = self._db['run_models']
        if self._validate_data(data):
            try:
                Collection.update_one({'_id': run_id}, {'$set': {key: data}}, upsert=True)
                logger.info(f"Run {key} updated successfully.")
            except Exception as e:
                logger.error(f"Error updating run {key}: {e}")

    def save_params(self, run_id: str, params: Dict[str, Any]) -> None:
        # Save the params to the params collection
        self._update_run_data(run_id, params, 'params')

    def save_metrics(self, run_id: str, metrics: Dict[str, Any]) -> None:
        # Save the metrics to the metrics collection
        self._update_run_data(run_id, metrics, 'metrics')

    def save_dataframe(self, run_id: str, df: pd.DataFrame) -> None:
        """Save a pandas DataFrame to MongoDb associated  with a specific run."""
        Collection = self._db['dataframes']
        if df.empty:
            logger.error("Invalid DataFrame. Cannot save an empty DataFrame.")
            return
        
        # Convert Dataframe to a list of dictionaries for MongoDB
        data = df.to_dict(orient='records')

        try:
            # Save the DataFrame tp a list of dicionaries for MongoDB
            Collection.insert_one({'run_id': run_id, 'data': data})
            logger.info(f"Dataframe for run {run_id} saved successfully.")
        except Exception as e:
            logger.error(f"Error saving DataFrame for run {run_id}: {e}")

