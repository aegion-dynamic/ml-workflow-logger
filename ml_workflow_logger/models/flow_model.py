import uuid
from typing import List, Dict, Any
from pydantic import BaseModel, Field, field_validator

# Class representing each step in the workflow
class StepModel(BaseModel):
    step_name: str
    step_info: str

    class Config:
        allow_population_by_field_name = True

# Class representing the flow of the ML workflow
class FlowModel(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()), alias='_id')
    name: str = ""
    steps: List[StepModel] = []

    @field_validator('name')
    def validate_name(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Flow name cannot be empty")
        return value
    
    def add_step(self, step_name: str, step_info: str):
        # Prevent duplicate step names
        if any(step.step_name == step_name for step in self.steps):
            raise ValueError(f"Step '{step_name}' already exists in the flow.")
        step = StepModel(step_name=step_name, step_info=step_info)
        self.steps.append(step)

    def to_dict(self) -> Dict[str, Any]:
        """Return a dictionary representation of the flow (with alias for '_id')."""
        return self.model_dump(by_alias=True, exclude_none=True)

    def to_dict_with_steps(self) -> Dict[str, Any]:
        """Returns a dictionary representation of the flow, including steps.
        """
        return {
            "_id": self.id,
            "name": self.name,
            "steps": [step.model_dump() for step in self.steps]
        }
    
    class Config:
        allow_population_by_field_name = True