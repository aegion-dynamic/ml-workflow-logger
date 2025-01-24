import uuid
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


# Class representing each step in the workflow
class StepModel(BaseModel):
    step_name: str
    step_data: Dict[str, Any] = {}

    class Config:
        population_by_name = True


# Class representing the flow of the ML workflow
class FlowModel(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()), alias="_id")
    name: str = ""
    steps: List[StepModel] = []
    status: Optional[str] = None

    @field_validator("name")
    def validate_name(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Flow name cannot be empty")
        return value

    def add_step(self, step_name: str, step_data: Dict[str, Any] = {}) -> None:
        """Adds a step to the flow, ensuring no duplicates."""
        # Prevent duplicate step names
        if any(step.step_name == step_name for step in self.steps):
            raise ValueError(f"Step '{step_name}' already exists in the flow.")
        step = StepModel(step_name=step_name, step_data=step_data)
        self.steps.append(step)

    def to_dict(self) -> Dict[str, Any]:
        """Return a dictionary representation of the flow (with alias for '_id')."""
        return self.model_dump(by_alias=True, exclude_none=True)

    def to_dict_with_steps(self) -> Dict[str, Any]:
        """Returns a dictionary representation of the flow, including steps."""
        return {
            "_id": self.id,
            "name": self.name,
            "status": self.status,
            "steps": [step.model_dump() for step in self.steps],
        }

    class Config:
        population_by_name = True
