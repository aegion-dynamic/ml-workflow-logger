from typing import Any, Dict
from pymongo import MongoClient
from ml_workflow_logger.drivers.abstract_driver import AbstractDriver
from ml_workflow_logger.drivers.abstract_driver import DBConfig
from ml_workflow_logger.flow import Flow
from ml_workflow_logger.models.flow_model import FlowModel, StepModel
from ml_workflow_logger.models.flow_record_model import FlowRecordModel
from ml_workflow_logger.models.run_model import RunModel
from ml_workflow_logger.run import Run


def _create_mongodb_client(config: DBConfig) -> MongoClient:
    return MongoClient(
        host=config.computed_connection_uri,
        username=config.username,
        password=config.password,
        port=config.port
    )



class MongoDBDriver(AbstractDriver):

    def __init__(self, db_config: DBConfig) -> None:

        self._client = _create_mongodb_client(db_config)
        self._db = self._client[db_config.database]

        # Check if all the required collections are present
        collections_to_check = ['flow_models', 'run_models', 'flowrecord_models', 'step_models']
        for collection in collections_to_check:
            if collection not in self._db.list_collection_names():
                self._db.create_collection(collection)
    
    def save_flow(self, flow_data: Flow) -> None:
        # Save the flow data to the flowmodels collection
        collection = self._db['flow_models']
        collection.insert_one(flow_data)

    
    def save_run(self, run_data: Run) -> None:
        # Save the run data to the runmodels collection
        collection = self._db['run_models']
        collection.insert_one(run_data)


    def save_step(self, step_data: StepModel) -> None:
        # Save the step data to the stepmodels collection
        collection = self._db['step_models']
        collection.insert_one(step_data)


    def save_flow_record(self, flow_record_data: FlowRecordModel) -> None:
        # Save the flow record data to the flowrecord_models collection
        collection = self._db['flowrecord_models']
        collection.insert_one(flow_record_data)
    

    def save_params(self, run_id: str, params: Dict[str, Any]) -> None:
        # Save the params to the params collection
        collection = self._db['run_models']
        collection.update_one({'_id': run_id}, {'$set': {'params': params}}, upsert=True)


    def save_metrics(self, run_id: str, metrics: Dict[str, Any]) -> None:
        # Save the metrics to the metrics collection
        collection = self._db['run_models']
        collection.update_one({'_id': run_id}, {'$set': {'metrics': metrics}}, upsert=True)
