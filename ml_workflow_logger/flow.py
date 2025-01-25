import uuid
from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional

import networkx as nx
from pydantic import ValidationError

from ml_workflow_logger.models.flow_model import FlowModel, StepModel


@dataclass
class Step:
    step_name: str
    step_data: Dict[str, Any]

    def to_dict(self) -> dict:
        return asdict(self)

@dataclass
class Flow:
    def __init__(self, flow_name: str):
        """Initializes the Flow object with flow name, run ID, and optional flow data."""
        self.flow_id: str = str(uuid.uuid4())
        self.flow_name: str = flow_name
        self.status: str = "created"
        self.steps: Dict[str, Step] = {}
        self.dag = nx.DiGraph()

    def add_step(self, step_name: str, step_data: Dict[str, Any] = {}):
        """Adds a step to the flow."""
        if step_name in self.steps:
            raise ValueError(f"Step '{step_name}' already exists in the flow.")

        # Create a Step object and add it to the flow steps dictionary
        step = Step(step_name=step_name, step_data=step_data)
        self.steps[step_name] = step
        self.dag.add_node(step_name)  # Add step to the DAG

    def update_step(self, step_name: str, step_data: Dict[str, Any]):
        """Updates data for an existing step."""
        if step_name not in self.steps:
            raise ValueError(f"Step '{step_name}' does not exist in the flow")

        step_to_edit = self.steps[step_name]
        step_to_edit.step_data.update(step_data)

    def validate(self):
        """Validates flow data before saving."""
        if not self.flow_name.strip():
            raise ValueError("Flow name cannot be empty.")
        if not self.steps:
            raise ValueError("At least one step must be added to the flow.")

    def to_model(self) -> FlowModel:
        """Converts the Flow object to a FlowModel."""
        try:
            # Validate the flow before conversion
            self.validate()

            flow_model = FlowModel(
                id=self.flow_id, name=self.flow_name, status=self.status  # Ensure status is set somewhere before this call
            )

            # Add all steps to the FlowModel
            for step in self.steps.values():
                flow_model.add_step(step_name=step.step_name, step_data=step.step_data)
            return flow_model
        except ValidationError as e:
            raise ValueError(f"Error converting to FlowModel: {e}")
        
    def to_dict(self) -> dict:
        """Converts the Flow object to a dictionary."""
        return {
            "flow_name": self.flow_name,
            "status": self.status,
            "steps": {step_name: step.to_dict() for step_name, step in self.steps.items()},
        }
