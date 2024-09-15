# from typing import Any, Dict, Optional

# from ml_workflow_logger.models.flow_model import FlowModel


# class Flow:
#     def __init__(self, flow_name: str, run_id: str, flow_data: Optional[Dict[str, Any]] = None):
#         self.flow_name = flow_name
#         self.run_id = run_id
#         self.flow_data = flow_data or {}

#     def add_step(self, step_name: str, step_info:str, step_data: Dict[str, Any] = {}):
#         raise NotImplementedError("Subclasses must implement this method")

#     def log_step(self, step_name: str, step_data: Dict[str, Any]):
#         raise NotImplementedError("Subclasses must implement this method")
    
#     def to_model(self) -> FlowModel:
#         ret = FlowModel(
#             name=self.flow_name
#             # TODO - Add All the other fields / steps
#         )

#         return ret

from typing import Any, Dict, Optional
from ml_workflow_logger.models.flow_model import FlowModel, StepModel

class Flow:
    def __init__(self, flow_name: str, run_id: str, flow_data: Optional[Dict[str, Any]] = None):
        """Initializes the Flow object with flow name, run ID, and optional flow data."""
        self.flow_name = flow_name
        self.run_id = run_id
        self.flow_data = flow_data or {"steps": []}  # Initialize with steps

    def add_step(self, step_name: str, step_info: str, step_data: Dict[str, Any] = {}):
        """Adds a step to the flow data."""
        # Add a new step to the flow_data['steps']
        step = {
            "step_name": step_name,
            "step_info": step_info,
            "step_data": step_data
            
        }
        self.flow_data["steps"].append(step)

    def log_step(self, step_name: str, step_data: Dict[str, Any]):
        """Logs data for an existing step."""
        # Search for the step in flow_data['steps'] and log the step_data
        for step in self.flow_data["steps"]:
            if step["step_name"] == step_name:
                step["step_data"].update(step_data)
                break
        else:
            raise ValueError(f"Step '{step_name}' not found in the flow.")

    def to_model(self) -> FlowModel:
        """Converts the Flow object to a FlowModel."""
        flow_model = FlowModel(
            name=self.flow_name
        )

        # Add all steps to the FlowModel
        for step in self.flow_data["steps"]:
            flow_model.add_step(
                step_name=step["step_name"],
                step_info=step["step_info"]
            )

        return flow_model