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
    name: str
    steps: List[Step] = []

    def add_step(self, step_name: str, step_info: str):
        step = Step(step_name=step_name, step_info=step_info)
        self.steps.append(step)

    def to_dict(self) -> Dict[str, Any]:
        return self.Dict(by_alias=True, exclude_none=True)
