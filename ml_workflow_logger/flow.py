import uuid
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional

import networkx as nx
from pydantic import ValidationError

from ml_workflow_logger.models.flow_model import FlowModel, StepModel


@dataclass
class Step:
    step_name: str
    step_data: Dict[str, Any]

    def to_dict(self) -> dict:
        return asdict(self)

    def to_model(self) -> StepModel:
        return StepModel(step_name=self.step_name, step_data=self.step_data)


@dataclass
class Flow:
    def __init__(self, flow_name: str):
        """Initializes the Flow object with flow name, run ID, and optional flow data."""
        self._flow_id: str = str(uuid.uuid4())
        self.flow_name: str = flow_name
        self.status: str = "created"
        self.steps: List[Step] = []
        self.dag = nx.DiGraph()

    @property
    def flow_id(self) -> str:
        """Returns the flow ID.

        Returns:
            str: The flow ID.
        """
        return self._flow_id

    def add_step(self, step_name: str, step_data: Dict[str, Any] = {}):
        """Adds a step to the flow, ensuring no duplicates.

        Args:
            step_name (str): The name of the step.
            step_data (Dict[str, Any], optional): Key value pair data we might want to store for the step. Defaults to {}.

        Raises:
            ValueError: If the step already exists in the flow.
        """
        if any(step.step_name == step_name for step in self.steps):
            raise ValueError(f"Step '{step_name}' already exists in the flow.")

        step = Step(step_name=step_name, step_data=step_data)
        self.steps.append(step)
        self.dag.add_node(step_name)  # Add step to the DAG

    def update_step(self, step_name: str, step_data: Dict[str, Any]):
        """Updates data for an existing step."""
        for step in self.steps:
            if step.step_name == step_name:
                step.step_data.update(step_data)
                return
        raise ValueError(f"Step '{step_name}' does not exist in the flow")

    def validate(self):
        """Validates flow data before saving."""
        if not self.flow_name.strip():
            raise ValueError("Flow name cannot be empty.")
        if not self.steps:
            raise ValueError("At least one step must be added to the flow.")

    def to_dict(self) -> dict:
        """Converts the Flow object to a dictionary."""
        return {
            "flow_id": self._flow_id,
            "flow_name": self.flow_name,
            "status": self.status,
            "steps": [step.to_dict() for step in self.steps],
        }

    def update_status(self, status: str):
        """Updates the status of the flow."""
        valid_statuses = {"created", "running", "completed", "failed"}
        if status not in valid_statuses:
            raise ValueError(
                f"Invalid status '{status}'. Valid statuses are: {valid_statuses}")
        self.status = status
        print(f"Flow '{self._flow_id}' status updated to '{self.status}'.")

    def end_flow(self):
        """Marks the end of the flow."""
        self.update_status("completed")
        # Optionally save flow state (e.g., save to file or update DB)
