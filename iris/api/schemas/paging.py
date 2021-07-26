from typing import Generic, List, Optional, TypeVar

from pydantic import AnyHttpUrl
from pydantic.generics import GenericModel

T = TypeVar("T")


class Paginated(GenericModel, Generic[T]):
    count: int
    next: Optional[AnyHttpUrl] = None
    previous: Optional[AnyHttpUrl] = None
    results: List[T]
