from uuid import UUID

import pydantic
import sqlmodel


class BaseModel(pydantic.BaseModel):
    class Config:
        allow_mutation = False
        # Temporary disabled to ease the migration to Iris 1.0
        # extra = pydantic.Extra.forbid
        json_encoders = {UUID: lambda v: str(v)}


class SQLModel(sqlmodel.SQLModel):
    class Config:
        extra = pydantic.Extra.forbid
        json_encoders = {UUID: lambda v: str(v)}
