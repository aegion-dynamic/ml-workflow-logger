#import uuid
#from pydantic import BaseModel, Field


#class Flow(BaseModel):
#    id: str = Field(alias='_id', default_factory=uuid.uuid4)
#    name: str = Field(alias='name')
#    pass