# test_drivers.py

import pytest
from unittest.mock import MagicMock, patch
from pymongo import errors
from ml_workflow_logger.drivers.abstract_driver import DBConfig, DBType
from ml_workflow_logger.drivers.mongodb import MongoDBDriver
from ml_workflow_logger.run import Run
from ml_workflow_logger.flow import Flow, Step
from typing import Dict, Optional, Any
import pandas as pd

# Assuming that the Run, Flow, and Step classes have appropriate methods like `dict()` and `to_model()`

@pytest.fixture
def db_config():
    """Fixture to create a DBConfig object for testing."""
    return DBConfig(
        database='test_ml_workflows',
        collection="test_logs",  # This is not used in the updated driver, but kept for completeness
        db_type=DBType.MONGO,
        host='localhost',
        port=27017,
        username='test_user',
        password='test_password'
    )

@pytest.fixture
def mock_mongo_driver(db_config):
    """
    Fixture to create a mock MongoDB driver with mocked methods and collections.
    It patches MongoClient and ensures that each collection is mocked appropriately.
    """
    with patch('ml_workflow_logger.drivers.mongodb.MongoClient') as MockClient:
        mock_client = MagicMock()
        mock_db = MagicMock()
        
        # Mock the behavior of accessing collections
        mock_db.__getitem__.side_effect = lambda name: mock_db_collections[name]
        
        # Create mock collections
        mock_db_collections = {
            'flow_models': MagicMock(),
            'run_models': MagicMock(),
            'flowrecord_models': MagicMock(),
            'step_models': MagicMock(),
            'dataframes': MagicMock()
        }
        
        mock_client.__getitem__.return_value = mock_db
        mock_db.list_collection_names.return_value = []  # Initially, no collections
        
        # Instantiate the driver with the mocked client
        driver = MongoDBDriver(db_config)
        
        # Pre-create collections in the mock
        for collection_name in ['flow_models', 'run_models', 'flowrecord_models', 'step_models', 'dataframes']:
            mock_db.list_collection_names.return_value = [collection_name]
            mock_db_collections[collection_name].create_index.return_value = None  # Mock index creation
        
        yield driver, mock_db_collections

class TestMongoDBDriver:
    def test_save_new_run_success(self, mock_mongo_driver):
        """Test saving a new run to MongoDB successfully."""
        driver, mock_collections = mock_mongo_driver
        run = Run(run_name="test_run", run_id="run_1")

        # Act
        driver.save_new_run(run)

        # Assert: Check if `insert_one` was called on 'run_models' collection with the correct data
        mock_collections['run_models'].insert_one.assert_called_once_with(run.model_dump())

    def test_save_new_run_duplicate_key(self, mock_mongo_driver):
        """Test saving a new run with a duplicate run_id, expecting a DuplicateKeyError."""
        driver, mock_collections = mock_mongo_driver
        run = Run(run_name="test_run", run_id="run_1")

        # Configure `insert_one` to raise DuplicateKeyError
        mock_collections['run_models'].insert_one.side_effect = errors.DuplicateKeyError("Duplicate key error")

        # Act & Assert
        with pytest.raises(errors.DuplicateKeyError):
            driver.save_new_run(run)

    def test_save_flow_success(self, mock_mongo_driver):
        """Test saving a flow to MongoDB successfully."""
        driver, mock_collections = mock_mongo_driver
        flow = Flow(flow_name="test_flow", run_id="run_1", flow_data={"key": "value"})

        # Act
        driver.save_flow(flow)

        # Assert: Check if `insert_one` was called on 'flow_models' collection with the correct data
        mock_collections['flow_models'].insert_one.assert_called_once_with(flow.to_model())

    def test_save_flow_invalid_run_id(self, mock_mongo_driver):
        """Test saving a flow with missing run_id, expecting ValueError."""
        driver, mock_collections = mock_mongo_driver
        flow = Flow(flow_name="test_flow", run_id='run_id', flow_data={"key": "value"})

        # Act & Assert
        with pytest.raises(ValueError, match="Invalid data: 'run_id' is missing or None."):
            driver.save_flow(flow)

    def test_save_metrics_success(self, mock_mongo_driver):
        """Test saving metrics to MongoDB successfully."""
        driver, mock_collections = mock_mongo_driver
        run_id = "run_1"
        metrics = {"accuracy": 0.95}

        # Act
        driver.save_metrics(run_id, metrics)

        # Assert: Check if `update_one` was called on 'run_models' collection with the correct data
        mock_collections['run_models'].update_one.assert_called_once_with(
            {'run_id': run_id},
            {'$set': {'metrics': metrics}},
            upsert=False
        )

    def test_save_metrics_missing_run_id(self, mock_mongo_driver):
        """Test saving metrics with missing run_id, expecting ValueError."""
        driver, mock_collections = mock_mongo_driver
        run_id = ""  # Invalid run_id
        metrics = {"accuracy": 0.95}

        # Act & Assert
        with pytest.raises(ValueError, match="run_id must be a valid, non-empty string."):
            driver.save_metrics(run_id, metrics)

    def test_save_metrics_no_run_found(self, mock_mongo_driver):
        """Test saving metrics when no run is found, expecting KeyError."""
        driver, mock_collections = mock_mongo_driver
        run_id = "non_existent_run"
        metrics = {"accuracy": 0.95}

        # Configure `update_one` to return matched_count=0
        mock_collections['run_models'].update_one.return_value.matched_count = 0

        # Act & Assert
        with pytest.raises(KeyError, match=f"No run found with run_id: {run_id}"):
            driver.save_metrics(run_id, metrics)

    def test_save_step_success(self, mock_mongo_driver):
        """Test saving a step to MongoDB successfully."""
        driver, mock_collections = mock_mongo_driver
        step_name = "step_1"
        step_data = {
            "run_id": "run_1",
            "step_name": step_name,
            "step_data": {"param": "value"}
        }

        # Add the step to the driver's internal steps
        flow_id = "flow_1"
        step = Step(flow_id=flow_id, step_name=step_name, step_data=step_data)
        driver.add_step(flow_id, step_name, step)

        # Act
        driver.save_step(step_name, step_data)

        # Assert: Check if `insert_one` was called on 'step_models' collection with the correct data
        mock_collections['step_models'].insert_one.assert_called_once_with(step_data)

    def test_missing_run_id(self, mock_mongo_driver):
        """Test saving a step with missing run_id, expecting ValueError."""
        driver, mock_collections = mock_mongo_driver
        step_name = "step_1"
        step_data = {
            "step_name": step_name,
            "step_data": {"param": "value"}
            # Missing 'run_id'
        }

        # Add the step without run_id
        flow_id = "flow_1"
        step = Step(flow_id=flow_id, step_name=step_name, step_data=step_data)
        driver.add_step(flow_id, step_name, step)

        # Act & Assert
        with pytest.raises(ValueError, match="'run_id' must be present and non-null in step_data."):
            driver.save_step(step_name, step_data)

    def test_save_flow_record_success(self, mock_mongo_driver):
        """Test saving a flow record to MongoDB successfully."""
        driver, mock_collections = mock_mongo_driver
        run_id = "run_1"
        step_name = "step_1"
        flow_record_data = {
            "run_id": run_id,
            "step_name": step_name,
            "flow_data": {"key": "value"}
        }

        # Act
        driver.save_flow_record(run_id, step_name, flow_record_data)

        # Assert: Check if `insert_one` was called on 'flowrecord_models' collection with the correct data
        mock_collections['flowrecord_models'].insert_one.assert_called_once_with(flow_record_data)

    def test_save_flow_record_missing_run_id(self, mock_mongo_driver):
        """Test saving a flow record with missing run_id, expecting ValueError."""
        driver, mock_collections = mock_mongo_driver
        run_id = ""
        step_name = "step_1"
        flow_record_data = {
            "step_name": step_name,
            "flow_data": {"key": "value"}
            # Missing 'run_id'
        }

        # Act & Assert
        with pytest.raises(ValueError, match="'run_id' must be present and non-null in step_data."):
            driver.save_flow_record(run_id, step_name, flow_record_data)

    def test_save_dataframe_success(self, mock_mongo_driver):
        """Test saving a dataframe to MongoDB successfully."""
        driver, mock_collections = mock_mongo_driver
        run_id = "run_1"
        df = MagicMock(spec=pd.DataFrame)
        df.empty = False
        df.to_dict.return_value = [{"column1": "value1"}, {"column1": "value2"}]

        # Act
        driver.save_dataframe(run_id, df)

        # Assert: Check if `insert_one` was called on 'dataframes' collection with the correct data
        expected_data = {
            'run_id': run_id,
            'data': [{"column1": "value1"}, {"column1": "value2"}]
        }
        mock_collections['dataframes'].insert_one.assert_called_once_with(expected_data)

    def test_save_dataframe_empty(self, mock_mongo_driver):
        """Test saving an empty dataframe, expecting ValueError."""
        driver, mock_collections = mock_mongo_driver
        run_id = "run_1"
        df = MagicMock(spec=pd.DataFrame)
        df.empty = True

        # Act & Assert
        with pytest.raises(ValueError, match="Cannot save an empty DataFrame."):
            driver.save_dataframe(run_id, df)

    def test_update_run_status_success(self, mock_mongo_driver):
        """Test updating the run status successfully."""
        driver, mock_collections = mock_mongo_driver
        run_id = "run_1"
        status = "completed"

        # Configure `update_one` to indicate a successful update
        mock_collections['run_models'].update_one.return_value.matched_count = 1

        # Act
        driver.update_run_status(run_id, status)

        # Assert: Check if `update_one` was called on 'run_models' collection with the correct data
        mock_collections['run_models'].update_one.assert_called_once_with(
            {'run_id': run_id},
            {'$set': {'status': status}},
            upsert=False
        )

    def test_update_run_status_no_run_found(self, mock_mongo_driver):
        """Test updating run status when no run is found, expecting KeyError."""
        driver, mock_collections = mock_mongo_driver
        run_id = "non_existent_run"
        status = "completed"

        # Configure `update_one` to indicate no matching document
        mock_collections['run_models'].update_one.return_value.matched_count = 0

        # Act & Assert
        with pytest.raises(KeyError, match=f"No run found with run_id: {run_id}"):
            driver.update_run_status(run_id, status)

    def test_update_run_status_missing_run_id(self, mock_mongo_driver):
        """Test updating run status with missing run_id, expecting ValueError."""
        driver, mock_collections = mock_mongo_driver
        run_id = ""
        status = "completed"

        # Act & Assert
        with pytest.raises(ValueError, match="run_id must be a valid, non-empty string."):
            driver.update_run_status(run_id, status)

    def test_save_step_without_add_step(self, mock_mongo_driver):
        """Test saving a step that hasn't been added to the driver's internal steps, expecting ValueError."""
        driver, mock_collections = mock_mongo_driver
        step_name = "step_1"
        step_data = {
            "run_id": "run_1",
            "step_name": step_name,
            "step_data": {"param": "value"}
        }

        # Act & Assert: Attempt to save a step without adding it first
        with pytest.raises(ValueError, match=f"Step '{step_name}' has not been added and cannot be saved."):
            driver.save_step(step_name, step_data)

    def test_save_step_missing_run_id(self, mock_mongo_driver):
        """Test saving a step with missing run_id, expecting ValueError."""
        driver, mock_collections = mock_mongo_driver
        step_name = "step_1"
        step_data = {
            "step_name": step_name,
            "step_data": {"param": "value"}
            # Missing 'run_id'
        }

        # Add the step without run_id
        flow_id = "flow_1"
        step = Step(flow_id=flow_id, step_name=step_name, step_data=step_data)
        driver.add_step(flow_id, step_name, step)

        # Act & Assert
        with pytest.raises(ValueError, match="'run_id' must be present and non-null in step_data."):
            driver.save_step(step_name, step_data)
