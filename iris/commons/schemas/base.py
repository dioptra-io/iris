from uuid import UUID

import pydantic
from sqlalchemy import func, types


class BaseModel(pydantic.BaseModel):
    class Config:
        allow_mutation = False
        extra = pydantic.Extra.forbid
        json_encoders = {UUID: lambda v: str(v)}


class ListType(types.TypeDecorator):
    """Store Python lists as strings of comma-delimited elements."""

    impl = types.Unicode
    cache_ok = True

    class comparator_factory(types.Unicode.Comparator):
        def contains(self, other, **kwargs):
            return func.like(f"%,{other},%", self.expr)

    def process_bind_param(self, value, dialect):
        if value:
            return f",{','.join(str(x) for x in value)},"
        return None

    def process_result_value(self, value, dialect):
        if value:
            return value.strip(",").split(",")
        return None
