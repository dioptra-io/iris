from uuid import UUID

import pydantic


class BaseModel(pydantic.BaseModel):
    class Config:
        json_encoders = {UUID: lambda v: str(v)}
