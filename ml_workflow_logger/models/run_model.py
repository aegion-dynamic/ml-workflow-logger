import uuid
from pydantic import BaseModel, Field, field_validator, ValidationInfo 
from typing import Dict, Any, Optional
from datetime import datetime
from ml_workflow_logger.models.flow_model import FlowModel


# Stores the metrics, when it starts, ends, and collects all the step data for a particular run
class RunModel(BaseModel):
    run_id: str = Field(alias='_id', default_factory=lambda: str(uuid.uuid4()))
    name: Optional[str] = Field(default=None)
    start_time: datetime = Field(default_factory=datetime.now)
    end_time: Optional[datetime] = Field(default=None)
    metrics: Dict[str, Any] = Field(default_factory=dict)
    flow_ref: Optional[FlowModel] = Field(default=None)
    status: str = Field(default="created")

    @field_validator('end_time', mode='before')
    def validate_end_time(cls, end_time: Optional[datetime], info: ValidationInfo) -> Optional[datetime]:
        if end_time and end_time < info.data['start_time']:
            raise ValueError("end_time cannot be earlier than start_time.")
        return end_time
    
    @field_validator('status')
    def validate_status(cls, status: str) -> Any:
        """Ensure that the provided status is valid.

        Args:
            status (str): _description_

        Raises:
            ValueError: _description_

        Returns:
            Any: _description_
        """
        valid_statuses = {"created", "running", "completed", "failed"}
        if status not in valid_statuses:
            raise ValueError(f"Invalid status '{status}'. Valid statuses are: {valid_statuses}")
        return status

    @field_validator('name', mode='before')
    def validate_name(cls, name: Optional[str]) -> Optional[str]:
        if name is not None and not name.strip():
            raise ValueError("Run name cannot be empty if provided.")
        return name

    def to_dict(self) -> Dict[str, Any]:
        """Converts the RunModel to a dictionary, using aliases and excluding None fields."""
        return self.model_dump(by_alias=True, exclude_none=True)

    def to_dict_with_flow(self) -> Dict[str, Any]:
        """Returns a dictionary including the flow details."""
        flow_dict = self.flow_ref.to_dict_with_steps() if self.flow_ref else None
        run_dict = self.to_dict()
        run_dict['flow'] = flow_dict
        return run_dict

    def complete_run(self, end_time: Optional[datetime] = None) -> None:
        """Marks the run as complete, setting the end_time."""
        self.end_time = end_time or datetime.now()
        self.status = "completed"

    # TODO: Implement saving all the run related objects and the corresponding flow
