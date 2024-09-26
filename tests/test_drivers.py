import pytest
from ml_workflow_logger.drivers.abstract_driver import DBConfig, DBType
from ml_workflow_logger.drivers.mongodb import MongoDBDriver
from ml_workflow_logger.run import Run
from ml_workflow_logger.flow import Flow
from unittest.mock import MagicMock

# Define a proper DBConfig object for testing purposes
@pytest.fixture
def db_config():
    return DBConfig(
        database='test_ml_workflows',
        collection="test_logs",
        db_type=DBType.MONGO,
        host='localhost',
        port=27017,
        username='test_user',
        password='test_password'
    )

@pytest.fixture
def mock_mongo_driver(db_config):
    """Fixture to create a mock MongoDB driver with mocked methods."""
    driver = MongoDBDriver(db_config)

    # Mock the methods that interact with the MongoDB client instead of the client itself
    mock_client = MagicMock()
    mock_collection = MagicMock()

    # Mock MongoDB client's collection
    mock_client[db_config.database][db_config.collection] = mock_collection
    driver._client = mock_client
    
    
    return driver, mock_collection

class TestMongoDBDriver:
    def test_save_run(self, mock_mongo_driver):
        """Test saving a run to MongoDB."""
        driver, mock_collection = mock_mongo_driver
        run = Run(run_name="test_run", run_id="run_1")

        # Act
        driver.save_new_run(run)

        # Assert: Check if `insert_one` was called on the mock collection with the correct data
        mock_collection.insert_one.assert_called_once_with({
            'run_name': run.run_name,
            'run_id': run.run_id,
        })

    def test_save_flow(self, mock_mongo_driver):
        """Test saving a flow to MongoDB."""
        driver, mock_collection = mock_mongo_driver
        flow = Flow(flow_name="test_flow", run_id="run_1", flow_data={})

        # Act
        driver.save_flow(flow)

        # Assert: Check if `insert_one` was called with the correct flow data
        mock_collection.insert_one.assert_called_once_with({
            'flow_name': flow.flow_name,
            'run_id': flow.run_id,
            'flow_data': flow.flow_data
        })

    def test_save_metrics(self, mock_mongo_driver):
        """Test saving metrics to MongoDB."""
        driver, mock_collection = mock_mongo_driver
        run_id = "run_1"
        metrics = {"accuracy": 0.95}

        # Act
        driver.save_metrics(run_id, metrics)

        # Assert: Check if `insert_one` was called with the correct metrics data
        mock_collection.insert_one.assert_called_once_with({
            'run_id': run_id,
            'metrics': metrics
        })

    def test_save_step(self, mock_mongo_driver):
        """Test saving a step to MongoDB."""
        driver, mock_collection = mock_mongo_driver
        step_name = "step_1"
        step_data = {"key": "value"}

        # Act
        driver.save_step(step_name, step_data)

        # Assert: Check if `insert_one` was called with the correct step data
        mock_collection.insert_one.assert_called_once_with({
            'step_name': step_name,
            'step_data': step_data
        })
