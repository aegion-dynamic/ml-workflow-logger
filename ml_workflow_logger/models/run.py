import uuid
from pydantic import BaseModel, Field

# Stores the metrics, when its starts, ends and allows us to collect all the step data for a particular run
class Run(BaseModel):
   id: str = Field(alias='_id', default_factory=uuid.uuid4)
   name: str = Field(alias='name')

    # TODO: Save all the run related objects and the corresponding flow
   pass