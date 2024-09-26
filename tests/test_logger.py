import pytest
import pandas as pd
from pathlib import Path
from ml_workflow_logger.logger import MLWorkFlowLogger
from ml_workflow_logger.drivers.mongodb import MongoDBDriver
from ml_workflow_logger.flow import Flow, Step
from ml_workflow_logger.run import Run
from ml_workflow_logger.drivers.abstract_driver import DBConfig, DBType

@pytest.fixture
def logger_instance():
    """Fixture to create an instance of MLWorkFlowLogger."""
    log_dir = "test_logs"
    db_config = DBConfig(
        database='test_db',
        collection="test_collection",
        db_type=DBType.MONGO,
        host='localhost',
        port=27017,
        username='root',
        password='password',
    )
    driver = MongoDBDriver(db_config)
    return MLWorkFlowLogger()


def test_add_new_flow(logger_instance):
    run_id = "test_run"
    flow_name = "test_flow"
    flow_data = {"param1": "value1"}

    logger_instance.add_new_flow(flow_name, run_id, flow_data)
    
    # Assert the flow was added successfully
    assert flow_name in logger_instance._flows
    assert logger_instance._flows[flow_name].flow_data == flow_data


def test_add_new_step(logger_instance):
    flow_id = "test_flow"
    step_name = "test_step"
    step_data = {"step_param": "step_value"}
    step_object = Step(flow_id, step_name, step_data)

    logger_instance.add_new_step(flow_id, step_name, step_object, step_data)
    
    # Assert the step was logged successfully
    assert step_name in [step.step_name for step in logger_instance._flows[flow_id].steps]


def test_start_new_run(logger_instance):
    run_name = "test_run"
    run_id = "12345"
    
    result_run_id = logger_instance.start_new_run(run_name, run_id)
    
    # Assert run is started and run_id is returned
    assert result_run_id == run_id
    assert run_id in logger_instance._runs


def test_log_metrics(logger_instance):
    run_id = "test_run"
    metrics = {"accuracy": 0.95}

    logger_instance.log_metrics(run_id, metrics)
    
    # Assert the metrics are saved successfully
    assert logger_instance._runs[run_id].metrics == metrics


def test_save_flow_record(logger_instance):
    run_id = "test_run"
    step_name = "test_step"
    step_data = {"step_output": "output_value"}

    logger_instance.save_flow_record(run_id, step_name, step_data)
    
    # Assert the flow record is saved
    assert step_name in logger_instance._runs[run_id].steps


def test_save_dataframe(logger_instance):
    run_id = "test_run"
    data = {"col1": [1, 2], "col2": [3, 4]}
    df = pd.DataFrame(data)

    logger_instance.save_dataframe(run_id, df)
    
    # Assert the dataframe was saved as a CSV file
    saved_file = Path(f"{run_id}_data.csv")
    assert saved_file.exists()
