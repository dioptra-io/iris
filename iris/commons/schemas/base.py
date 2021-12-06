from uuid import UUID

import pydantic


class BaseModel(pydantic.BaseModel):
    class Config:
        allow_mutation = False
        extra = pydantic.Extra.forbid
        json_encoders = {UUID: lambda v: str(v)}
