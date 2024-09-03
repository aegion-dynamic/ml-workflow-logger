import uuid
from pydantic import BaseModel, Field
from typing import Dict, Any
from datetime import datetime
from ml_workflow_logger.models.flow_model import FlowModel

# Stores the metrics, when its starts, ends and allows us to collect all the step data for a particular run
class RunModel(BaseModel):
   id: str = Field(alias='_id', default_factory=lambda: str(uuid.uuid4()))
   name: str = Field(default=None)
   start_time: datetime =Field(default_factory=datetime.now)
   end_time: datetime = Field(default=None)
   params: Dict[str, Any] = Field(default_factory=dict)
   metrics: Dict[str, Any] = Field(default_factory=dict)
   flow_ref: FlowModel = Field(default=None)

   def to_dict(self) -> Dict[str, Any]:
      return self.model_dump(by_alias=True, exclude_none=True)
   
    # TODO: Save all the run related objects and the corresponding flow