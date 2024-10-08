# test_logger.py

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from ml_workflow_logger.drivers.abstract_driver import DBConfig, DBType
from ml_workflow_logger.drivers.mongodb import MongoDBDriver
from ml_workflow_logger.flow import Flow, Step
from ml_workflow_logger.logger import MLWorkFlowLogger
from ml_workflow_logger.run import Run


@pytest.fixture
def mock_mongo_driver():
    """
    Fixture to create a mocked MongoDBDriver.
    This prevents actual database operations during testing.
    """
    mock_driver = MagicMock(spec=MongoDBDriver)
    return mock_driver


@pytest.fixture
def logger_instance(mock_mongo_driver):
    """
    Fixture to create an instance of MLWorkFlowLogger with a mocked MongoDBDriver.
    Returns both the logger instance and the mocked driver for assertions.
    """
    log_dir = Path("test_logs")  # Use a Path object for log_dir
    logger = MLWorkFlowLogger(db_driver=mock_mongo_driver, log_dir=log_dir)
    return logger, mock_mongo_driver


def test_add_new_flow(logger_instance):
    """
    Test adding a new flow to MLWorkFlowLogger.
    Ensures that the driver.save_flow method is called with the correct Flow object.
    """
    logger, mock_driver = logger_instance
    run_id = "test_run"
    flow_name = "test_flow"
    flow_data = {"param1": "value1"}

    # Act
    logger.add_new_flow(flow_name, run_id, flow_data)

    # Assert: Check that driver.save_flow was called once with a Flow instance
    mock_driver.save_flow.assert_called_once()
    saved_flow = mock_driver.save_flow.call_args[0][0]  # Retrieve the first argument passed
    assert isinstance(saved_flow, Flow)
    assert saved_flow.flow_name == flow_name
    assert saved_flow.run_id == run_id
    assert saved_flow.flow_data == flow_data


def test_add_new_step(logger_instance):
    """
    Test adding a new step to MLWorkFlowLogger.
    Ensures that the driver.save_step method is called with the correct step data.
    """
    logger, mock_driver = logger_instance
    flow_id = "test_flow"
    step_name = "test_step"
    step_data = {"step_param": "step_value"}
    step_object = Step(flow_id, step_name, step_data)

    # Act
    logger.add_new_step(flow_id, step_name, step_object, step_data)

    # Assert: Check that driver.save_step was called once with correct arguments
    mock_driver.save_step.assert_called_once_with(step_name, step_data)


def test_start_new_run(logger_instance):
    """
    Test starting a new run in MLWorkFlowLogger.
    Ensures that the driver.save_new_run method is called with the correct Run object
    and that the run_id is correctly returned and stored.
    """
    logger, mock_driver = logger_instance
    run_name = "test_run"
    run_id = "12345"

    # Act
    result_run_id = logger.start_new_run(run_name, run_id)

    # Assert: Check that driver.save_new_run was called once with a Run instance
    mock_driver.save_new_run.assert_called_once()
    saved_run = mock_driver.save_new_run.call_args[0][0]  # Retrieve the first argument passed
    assert isinstance(saved_run, Run)
    assert saved_run.run_name == run_name
    assert saved_run.run_id == run_id

    # Check that run_id is returned and stored in logger._runs
    assert result_run_id == run_id
    assert run_id in logger._runs  # Assuming _runs is accessible


def test_log_metrics(logger_instance):
    """
    Test logging metrics in MLWorkFlowLogger.
    Ensures that the driver.save_metrics method is called with the correct arguments.
    """
    logger, mock_driver = logger_instance
    run_id = "test_run"
    metrics = {"accuracy": 0.95}

    # Act
    logger.log_metrics(run_id, metrics)

    # Assert: Check that driver.save_metrics was called once with correct arguments
    mock_driver.save_metrics.assert_called_once_with(run_id, metrics)

    # Optionally, check internal state if accessible
    # assert logger._runs[run_id].metrics == metrics


def test_save_flow_record(logger_instance):
    """
    Test saving a flow record in MLWorkFlowLogger.
    Ensures that the driver.save_flow_record method is called with the correct arguments.
    """
    logger, mock_driver = logger_instance
    run_id = "test_run"
    step_name = "test_step"
    step_data = {"step_output": "output_value"}

    # Act
    logger.save_flow_record(run_id, step_name, step_data)

    # Assert: Check that driver.save_flow_record was called once with correct arguments
    mock_driver.save_flow_record.assert_called_once_with(run_id, step_name, step_data)


def test_save_dataframe(logger_instance, tmp_path):
    """
    Test saving a dataframe in MLWorkFlowLogger.
    Ensures that the dataframe is saved as a CSV file and the driver.save_dataframe method is called.
    """
    logger, mock_driver = logger_instance
    run_id = "test_run"
    data = {"col1": [1, 2], "col2": [3, 4]}
    df = pd.DataFrame(data)

    # Patch the logger's log_dir to use the temporary path provided by pytest
    with patch.object(logger, "log_dir", tmp_path):
        # Act
        logger.save_dataframe(run_id, df)

        # Assert: Check that driver.save_dataframe was called once with correct arguments
        mock_driver.save_dataframe.assert_called_once_with(run_id, df)

        # Assert that the dataframe was saved as a CSV file in tmp_path
        saved_file = tmp_path / f"{run_id}_data.csv"
        assert saved_file.exists(), f"CSV file {saved_file} does not exist."

        # Optionally, verify the content of the saved CSV file
        saved_df = pd.read_csv(saved_file)
        pd.testing.assert_frame_equal(saved_df, df)
