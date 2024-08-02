
import uuid
from pydantic import BaseModel, Field

from ml_workflow_logger.models.flow import Flow


class FlowRecord(BaseModel):
    id: str = Field(alias='_id', default_factory=uuid.uuid4)
    # TODO: Fill out the other records you need to have to make this a standardized record
    # TODO: Create a reference to the Flow model
    flow_ref: Flow
    pass


