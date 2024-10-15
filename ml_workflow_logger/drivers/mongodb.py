import logging
from typing import Any, Dict

import pandas as pd
from pymongo import MongoClient, errors

from ml_workflow_logger.drivers.abstract_driver import AbstractDriver, DBConfig
from ml_workflow_logger.flow import Flow, Step
from ml_workflow_logger.models.flow_record_model import FlowRecordModel
from ml_workflow_logger.models.run_model import RunModel
from ml_workflow_logger.run import Run

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def _create_mongodb_client(db_config: DBConfig) -> MongoClient:
    """Create and return a MongoDB client based on the DB configuration."""
    try:
        client = MongoClient(
            host=db_config.computed_connection_uri,
            username=db_config.username,
            password=db_config.password,
            port=db_config.port,
            serverSelectionTimeoutMS=5000,  # 5 seconds timeout
        )
        # Attempt to connect to verify credentials and connection
        client.admin.command("ping")
        logger.info("Successfully connected to MongoDB")
        return client
    except errors.ConnectionFailure as e:
        logger.error(f"Could not connect to MongoDB: {e}")
        raise
    except Exception as e:
        logger.error(f"An error occured while creating MongoDB client: {e}")
        raise

    # return MongoClient(
    #     host=config.computed_connection_uri,
    #     username=config.username,
    #     password=config.password,
    #     port=config.port
    # )


class MongoDBDriver(AbstractDriver):
    """MongoDB Driver implementation of AbstractDriver."""

    def __init__(self, db_config: DBConfig) -> None:
        """Initialize MongoDB client and database.

        Args:
            config (DBConfig): Configure the databse
        """
        self.steps: Dict[str, Step] = {}

        try:
            self._client = _create_mongodb_client(db_config)
            self._db = self._client[db_config.database]

            # Check if all the required collections are present
            collections_to_check = ["flow_model", "run_models", "flowrecord_models", "step_models", "dataframes"]
            for collection in collections_to_check:
                if collection not in self._db.list_collection_names():
                    self._db.create_collection(collection)
                    logger.info("Created collection: {collection}")

            # Add index to important fields (e.g., run_id)
            self._db["run_models"].create_index("run_id", unique=True)
            logger.info("created index on 'run_id' for 'run_models' collection")

        except Exception as e:
            logger.error(f"Error initializing MongoDB: {e}")
            raise

    def _convert_to_dict(self, data: Any) -> Dict[str, Any]:
        """Convert model instances to dictionaries for MongoDB."""
        return data.dict() if hasattr(data, "dict") else data

    def _validate_data(self, data: Dict[str, Any]) -> bool:
        """Basic validation of data before saving to MongoDB.
        Ensures that 'run_id' is present and not None.

        Args:
            data (Dict[str, Any]): Data to validate

        Returns:
            bool: True if data is valid, False otherwise.
        """
        if not isinstance(data, dict) or not data:
            logger.error("Invalid data format. Expected a non-empty dictionary.")
            return False
        if "run_id" not in data or not data["run_id"]:
            logger.error("Invalid data: 'run_id' is missing or None.")
            return False
        return True

    def save_flow(self, flow_object: Flow) -> None:
        """Save the flow data to the flowmodels collection

        Args:
            flow_object (Flow): The Flow object to save.
        """
        collection = self._db["flow_models"]
        data = self._convert_to_dict(flow_object)

        if not self._validate_data(data):
            logger.error("Flow data validation failed. Flow not saved.")
            return

        try:
            collection.insert_one(data)
            logger.info("Flow data saved successfully for ru_id: %s.", data["run_id"])
        except errors.DuplicateKeyError:
            logger.error("Duplicate run_id '%s' detected while saving flow.", data["run_id"])
            raise
        except Exception as e:
            logger.error(f"Error saving flow data: {e}")
            raise

    def add_step(self, flow_id, step_name: str, step_data: Dict[str, Any] = {}) -> None:
        """Adds a step to the flow data.

        Args:
            flow_id (_type_): ID of the flow to which the step belongs.
            step_name (str): Name of the step
            step_data (Dict[str, Any], optional): Data related to the step. Defaults to {}.
        """
        if not flow_id:
            logger.error("Invalid flow_id provided for adding a step: None or empty.")
            raise ValueError("flow_id must be a valid, non-empty string.")

        # Create a Step object and add it to the flow steps dictionary
        step = Step(flow_name=flow_id, step_name=step_name, step_data=step_data)
        self.steps[step_name] = step
        logger.debug("Added step '%s' to flow '%s'.", step_name, flow_id)

    def save_step(self, step_name, step_data: Dict[str, Any]) -> None:
        """Save the step data to the step_models collection.

        Args:
            step_name (str): Name of the step.
            step_data (Dict[str, Any]): Data related to the step.
        Raises:
            ValueError: _description_
        """
        if step_name not in self.steps:
            logger.error("Step '%s' does not exist in the internal steps dictionary.", step_name)
            raise ValueError(f"Step '{step_name}' has not been added and cannot be saved")

        # Ensure that 'run_id' is present in step_data
        if "run_id" not in step_data or not step_data["run_id"]:
            logger.error("Cannot save step data: 'run_id' is missing or None in step_data.")
            raise ValueError("'run_id' must be present and non-null in step_data.")

        # Save the step data to the stepmodels collection
        collection = self._db["step_models"]
        data = self._convert_to_dict(step_data)

        try:
            collection.insert_one(data)
            logger.info("Step data saved successfully for step '%s' and run_id: %s.", step_name, data["run_id"])
        except errors.DuplicateKeyError:
            logger.error("Duplicate step detected for step '%s'.", step_name, data["run_id"])
            raise
        except Exception as e:
            logger.error(f"Error saving the step data: {e}")
            raise

    def save_new_run(self, run_object: Run) -> None:
        """Save the run data to the run_models collection.

        Args:
            run_object (Run): The Run object to save.
        """
        collection = self._db["run_models"]
        data = self._convert_to_dict(run_object)

        if self._validate_data(data):
            logger.error("Run data validation failed. Run not saved.")
            raise ValueError("Invalid run data. 'run_id' must be present and non-null.")

        try:
            collection.insert_one(data)
            logger.info("Run data saved successfully for run_id: %s.", data["run_id"])
        except errors.DuplicateKeyError:
            logger.error("Duplicate run_id '%s' detected while saving run.", data["run_id"])
            raise
        except Exception as e:
            logger.error(f"Error saving run_data: {e}")
            raise

    def _update_run_data(self, run_id: str, data: Dict[str, Any], key: str) -> None:
        """Utility method to update run data (params/metrics).

        Args:
            run_id (str): The run_id of the run to update.
            data (Dict[str, Any]): The data to update.
            key (str): The field to update (e.g., 'metrics', 'params').
        """
        if not run_id:
            logger.error("Cannot update run data: 'run_id' is None or empty.")
            raise ValueError("run_id must be a valid, non-empty string.")

        collection = self._db["run_models"]

        if not self._validate_data({"run_id": run_id}):
            logger.error("Run data validation failed for run_id: %s.", run_id)
            raise ValueError("Invalid run_id for updating run data.")

        try:
            result = collection.update_one(
                {"run_id": run_id}, {"$set": {key: data}}, upsert=False  # Do not insert if the document does not exist
            )
            if result.matched_count == 0:
                logger.error("No run found with run_id: %s to update.", run_id)
                raise KeyError(f"No run found with run_id: {run_id}")
            logger.info("Run '%s' updated successfully for run_id: %s.", key, run_id)
        except Exception as e:
            logger.error(f"Error updating run '{key}' for run_id '{run_id}': {e}")
            raise

    def save_metrics(self, run_id: str, metrics: Dict[str, Any]) -> None:
        """Save metrics for a specific run.

        Args:
            run_id (str): Run ID to which the metrics belong.
            metrics (Dict[str, Any]): Metrics data.
        """
        if not metrics:
            logger.error("Metrics data is empty. Cannot save empty metrics for run_id: %s.", run_id)
            raise ValueError("Metrics data must be a non-empty dictionary.")

        self._update_run_data(run_id, metrics, "metrics")

    def save_flow_record(self, run_id: str, step_name: str, step_data: Dict[str, Any]) -> None:
        """Save a flow record associated with a specific run and step.

        Args:
            run_id (str): Run ID associated with the flow record.
            step_name (str): Name of the step.
            step_data (Dict[str, Any]): Data related to the step.
        """
        if not run_id:
            logger.error("Invalid run_id provided for saving flow record: None or empty.")
            raise ValueError("run_id must be a valid, non-empty string.")

        if not step_name:
            logger.error("Invalid step_name provided for saving flow record: None or empty.")
            raise ValueError("step_name must be a valid, non-empty string.")

        # Ensure that 'run_id' is present in step_data
        if "run_id" not in step_data or not step_data["run_id"]:
            logger.error("Cannot save flow record data: 'run_id' is missing or None in step_data.")
            raise ValueError("'run_id' must be present and non-null in step_data.")

        collection = self._db["flowrecord_models"]
        data = self._convert_to_dict(step_data)

        try:
            collection.insert_one(data)
            logger.info("Flow record data saved successfully for step '%s' and run_id: %s.", step_name, data["run_id"])
        except errors.DuplicateKeyError:
            logger.error("Duplicate flow record detected for step '%s' and run_id: %s.", step_name, data["run_id"])
            raise
        except Exception as e:
            logger.error(f"Error saving flow record data: {e}")
            raise

    def save_dataframe(self, run_id: str, df: pd.DataFrame) -> None:
        """Save a pandas DataFrame to MongoDB associated with a specific run.

        Args:
            run_id (str): Run ID associated with the DataFrame.
            df (pd.DataFrame): DataFrame to be saved.
        """
        if not run_id:
            logger.error("Invalid run_id provided for saving DataFrame: None or empty.")
            raise ValueError("run_id must be a valid, non-empty string.")

        if df.empty:
            logger.error("Invalid DataFrame. Cannot save an empty DataFrame for run_id: %s.", run_id)
            raise ValueError("Cannot save an empty DataFrame.")

        collection = self._db["dataframes"]
        data = {"run_id": run_id, "data": df.to_dict(orient="records")}

        try:
            collection.insert_one(data)
            logger.info("DataFrame for run_id '%s' saved successfully.", run_id)
        except errors.DuplicateKeyError:
            logger.error("Duplicate DataFrame detected for run_id: %s.", run_id)
            raise
        except Exception as e:
            logger.error(f"Error saving DataFrame for run_id '{run_id}': {e}")
            raise

    def update_run_status(self, run_id: str, status: str) -> None:
        """Update the status of a specific run.

        Args:
            run_id (str): Run ID to update.
            status (str): New status (e.g., 'completed').
        """
        if not run_id:
            logger.error("Invalid run_id provided for updating run status: None or empty.")
            raise ValueError("run_id must be a valid, non-empty string.")

        if not status:
            logger.error("Invalid status provided for updating run status: None or empty.")
            raise ValueError("status must be a valid, non-empty string.")

        collection = self._db["run_models"]

        try:
            result = collection.update_one(
                {"run_id": run_id},
                {"$set": {"status": status}},
                upsert=False,  # Do not insert if the document does not exist
            )
            if result.matched_count == 0:
                logger.error("No run found with run_id: %s to update status.", run_id)
                raise KeyError(f"No run found with run_id: {run_id}")
            logger.info("Run status updated to '%s' for run_id: %s.", status, run_id)
        except Exception as e:
            logger.error(f"Error updating run status for run_id '{run_id}': {e}")
            raise
