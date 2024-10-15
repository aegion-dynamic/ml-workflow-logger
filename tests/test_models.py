import pytest

from ml_workflow_logger.models.flow_model import FlowModel
from ml_workflow_logger.models.flow_record_model import FlowRecordModel
from ml_workflow_logger.models.run_model import RunModel


class TestFlowModel:

    def test_flow_model(self) -> None:
        # Test creating a flow model
        flow = FlowModel(name="test_flow", status="created")
        assert flow.name == "test_flow"
        assert flow.status == "created"

    def test_flow_status_update(self) -> None:
        # Test updating flow model status
        flow = FlowModel(name="test_flow", status="created")
        flow.status = "running"  # Directly update the status
        assert flow.status == "running"

    def test_run_model(self) -> None:
        # Test creating a run model
        run = RunModel(status="pending")  # Ensure status is provided if needed
        assert run.run_id == "test_run"
        assert run.status == "pending"

    def test_run_status_update(self) -> None:
        # Test updating a run model status
        run = RunModel(status="pending")
        run.status = "running"  # Directly update the status
        assert run.status == "running"

    def test_flow_record_model(self) -> None:
        # Test creating a flow record model
        record_model = FlowRecordModel(name="test_record_model", status="pending")
        assert record_model.name == "test_record_model"
        assert record_model.status == "pending"

    def test_flow_record_status_update(self) -> None:
        # Test updating a flow_record model status
        record_model = FlowRecordModel(name="test_record_model", status="pending")
        record_model.status = "running"  # Directly update the status
        assert record_model.status == "running"
