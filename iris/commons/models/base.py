import pydantic
import sqlmodel
from sqlalchemy import func, types
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
    """
    Custom JSON type for Pydantic models.
    >>> from iris.commons.test import TestModel
    >>> t1 = PydanticType(TestModel)
    >>> v1 = TestModel(a=1)
    >>> t1.process_result_value(t1.process_bind_param(v1, None), None)
    TestModel(a=1)
    """

    impl = types.Unicode
    cache_ok = True

    def __init__(self, klass, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.klass = klass

    def process_bind_param(self, value, dialect):
        return value.json()

    def process_result_value(self, value, dialect):
        return self.klass.parse_raw(value)


class ListType(types.TypeDecorator):
    """
    Store Python lists as strings of comma-delimited elements.
    >>> t = ListType()
    >>> t.process_result_value(t.process_bind_param(None, None), None)
    >>> t.process_result_value(t.process_bind_param([], None), None)
    []
    >>> t.process_result_value(t.process_bind_param(["a", "b", "c"], None), None)
    ['a', 'b', 'c']
    """

    impl = types.Unicode
    cache_ok = True

    class comparator_factory(types.Unicode.Comparator):
        def contains(self, other, **kwargs):
            return func.like(f"%,{other},%", self.expr)

    def process_bind_param(self, value, dialect):
        if value is not None:
            return f",{','.join(str(x) for x in value)},"
        return None

    def process_result_value(self, value, dialect):
        if value == ",,":
            return []
        elif value:
            return value.strip(",").split(",")
        return None
