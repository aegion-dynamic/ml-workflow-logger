

from typing import Any, Dict, Optional

from ml_workflow_logger.models.flow_model import FlowModel


class Flow:
    def __init__(self, flow_name: str, run_id: str, flow_data: Optional[Dict[str, Any]] = None):
        self.flow_name = flow_name
        self.run_id = run_id
        self.flow_data = flow_data or {}

    def add_step(self, step_name: str, step_info:str, step_data: Dict[str, Any] = {}):
        raise NotImplementedError("Subclasses must implement this method")

    def log_step(self, step_name: str, step_data: Dict[str, Any]):
        raise NotImplementedError("Subclasses must implement this method")
    
    def to_model(self) -> FlowModel:
        ret = FlowModel(
            name=self.flow_name
            # TODO - Add All the other fields / steps
        )

        return ret