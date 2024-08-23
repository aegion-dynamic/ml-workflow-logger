import uuid
from typing import List, Dict, Any
from pydantic import BaseModel, Field

# Class representing each step in the workflow
class Step(BaseModel):
    step_name: str
    step_info: str

# Class representing the flow of the ML workflow
class Flow(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()), alias='_id')
    name: str = ""
    steps: List[Step] = []

    def add_step(self, step_name: str, step_info: str):
        step = Step(step_name=step_name, step_info=step_info)
        self.steps.append(step)

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump(by_alias=True, exclude_none=True)

    def to_dict_with_steps(self) -> Dict[str, Any]:
        """Returns a dictionary representation of the flow, including steps.
        """
        return {
            "_id": self.id,
            "name": self.name,
            "steps": [step.model_dump() for step in self.steps]
        }