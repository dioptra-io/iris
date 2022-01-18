import json

import pydantic
import sqlmodel
from sqlalchemy import types
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeMeta, declarative_base, registry

Base: DeclarativeMeta = declarative_base()  # type: ignore


class BaseModel(pydantic.BaseModel):
    class Config:
        extra = pydantic.Extra.forbid


class BaseSQLModel(sqlmodel.SQLModel, registry=registry(metadata=Base.metadata)):
    """
    Override of the SQLModel base class to use the same SQLAlchemy metadata object
    as FastAPI-Users.
    """


class PydanticType(types.TypeDecorator):
    impl = JSONB
    cache_ok = True

    def __init__(self, klass, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.klass = klass

    def process_bind_param(self, value, dialect):
        # Hack to recursively convert pydantic models to dict.
        return json.loads(value.json())

    def process_result_value(self, value, dialect):
        return self.klass.parse_obj(value)
