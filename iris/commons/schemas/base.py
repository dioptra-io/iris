from uuid import UUID

import pydantic
import sqlmodel


class BaseModel(pydantic.BaseModel):
    class Config:
        # allow_mutation = False
        # # NOTE: Deactivated because `ToolParameter` class
        # need to assign `n_initial_flows`
        extra = pydantic.Extra.forbid
        json_encoders = {UUID: lambda v: str(v)}


class SQLModel(sqlmodel.SQLModel):
    class Config:
        extra = pydantic.Extra.forbid
        json_encoders = {UUID: lambda v: str(v)}
