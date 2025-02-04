import uuid
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, field_validator


class FlowRecordModel(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()), alias="_id")
    step_name: str = ""
    step_data: Dict[str, Any] = Field(default_factory=dict)  # Default to an empty dict if not provided
    flow_ref: str
    run_ref: str

    def to_dict(self) -> Dict[str, Any]:
        """Converts the FlowRecordModel to a dictionary using aliases and excluding None fields."""
        return self.model_dump(by_alias=True, exclude_none=True)

    def to_dict_with_refs(self) -> Dict[str, Any]:
        """Returns a dictionary representation including the flow and run references."""
        # Call the respective to_dict methods on the flow and run references
        flow_dict = self.flow_ref.to_dict_with_steps() if self.flow_ref else None
        run_dict = self.run_ref.to_dict() if self.run_ref else None

        # Get the dictionary representation of the FlowRecordModel
        flow_record_dict = self.to_dict()

        # Add Flow and Run details to the dictionary
        flow_record_dict["flow"] = flow_dict
        flow_record_dict["run"] = run_dict

        return flow_record_dict

    @field_validator("step_name")
    def validate_step_name(cls, step_name: str) -> str:
        if not step_name.strip():
            raise ValueError("Step name cannot be empty.")
        return step_name
    
    @field_validator("flow_ref")
    def validate_flow_ref(cls, flow_ref: str) -> str:
        if not flow_ref:
            raise ValueError("Flow reference cannot be empty.")
        return flow_ref
    
    @field_validator("run_ref")
    def validate_run_ref(cls, run_ref: str) -> str:
        if not run_ref:
            raise ValueError("Run reference cannot be empty.")
        return run_ref
