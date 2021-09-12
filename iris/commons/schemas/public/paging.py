from typing import Generic, List, Optional, TypeVar

from pydantic import AnyHttpUrl, NonNegativeInt
from pydantic.generics import GenericModel

T = TypeVar("T")


class Paginated(GenericModel, Generic[T]):
    count: NonNegativeInt
    next: Optional[AnyHttpUrl] = None
    previous: Optional[AnyHttpUrl] = None
    results: List[T]
