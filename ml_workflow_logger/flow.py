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


@dataclass
class Flow:
    def __init__(self, flow_name: str):
        """Initializes the Flow object with flow name, run ID, and optional flow data."""
        self.local_flow_id: str = str(uuid.uuid4())
        self.global_flow_id: Optional[str] = None  # This will be set when the flow is saved on the database
        self._is_locked: bool = False
        self.flow_name: str = flow_name
        self.status: str = "created"
        self.steps: List[Step] = []
        self.dag = nx.DiGraph()

    def add_step(self, step_name: str, step_data: Dict[str, Any] = {}):
        """Adds a step to the flow, ensuring no duplicates.

        Args:
            step_name (str): The name of the step.
            step_data (Dict[str, Any], optional): Key value pair data we might want to store for the step. Defaults to {}.

        Raises:
            ValueError: If the step already exists in the flow.
        """
        # Check if flow is locked
        if self._is_locked:
            raise ValueError("Flow is locked and cannot be modified.")

        # Check if step already exists
        if any(step.step_name == step_name for step in self.steps):
            raise ValueError(f"Step '{step_name}' already exists in the flow.")

        step = Step(step_name=step_name, step_data=step_data)
        self.steps.append(step)
        self.dag.add_node(step_name)  # Add step to the DAG

    def add_transition(self, source: str, target: str):
        """Adds a transition between two steps in the flow.

        Args:
            source (str): The source step name.
            target (str): The target step name.

        Raises:
            ValueError: If the source or target step does not exist in the flow.
        """
        # Check if flow is locked
        if self._is_locked:
            raise ValueError("Flow is locked and cannot be modified.")

        # Check if source and target steps exist
        if source not in self.dag.nodes:
            raise ValueError(f"Step '{source}' does not exist in the flow.")
        if target not in self.dag.nodes:
            raise ValueError(f"Step '{target}' does not exist in the flow.")

        self.dag.add_edge(source, target)

    def generate_dag_hash(self) -> str:
        """Generates a hash for the flow DAG."""
        raise NotImplementedError("Method not implemented.")

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
            "flow_id": self.local_flow_id,
            "flow_name": self.flow_name,
            "status": self.status,
            "steps": [step.to_dict() for step in self.steps],
        }

    def update_status(self, status: str):
        """Updates the status of the flow."""
        valid_statuses = {"created", "running", "completed", "failed"}
        if status not in valid_statuses:
            raise ValueError(f"Invalid status '{status}'. Valid statuses are: {valid_statuses}")
        self.status = status
        print(f"Flow '{self.local_flow_id}' status updated to '{self.status}'.")

    def lock_flow(self):
        """Locks the flow to prevent further modifications."""
        self._is_locked = True
        print(f"Flow '{self.local_flow_id}' locked.")
