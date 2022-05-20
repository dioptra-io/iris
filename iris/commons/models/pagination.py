from typing import Generic, TypeVar

from pydantic import AnyHttpUrl, NonNegativeInt
from pydantic.generics import GenericModel
from starlette.datastructures import URL

T = TypeVar("T")


class Paginated(GenericModel, Generic[T]):
    """
    >>> url = URL("http://localhost:8000/test")
    >>> results = [1, 2, 3, 4]
    >>> p = Paginated.from_results(url, results, 10, 0, 4)
    >>> str(p.next)
    'http://localhost:8000/test?limit=4&offset=4'
    >>> str(p.previous)
    'None'
    >>> p = Paginated.from_results(url, results, 10, 8, 4)
    >>> str(p.next)
    'None'
    >>> str(p.previous)
    'http://localhost:8000/test?limit=4&offset=4'
    """

    count: NonNegativeInt
    next: AnyHttpUrl | None = None
    previous: AnyHttpUrl | None = None
    results: list[T]

    @classmethod
    def from_results(
        cls, url: URL, results: list[T], count: int, offset: int, limit: int
    ) -> "Paginated[T]":
        next_url = None
        prev_url = None
        if offset + limit < count:
            next_url = str(url.include_query_params(limit=limit, offset=offset + limit))
        if offset - limit > 0:
            prev_url = str(url.include_query_params(limit=limit, offset=offset - limit))
        return Paginated(count=count, next=next_url, previous=prev_url, results=results)
