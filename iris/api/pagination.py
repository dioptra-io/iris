"""Pagination abstract class."""

from abc import ABC, abstractmethod
from typing import Optional


class Pagination(ABC):
    def __init__(self, request, offset, limit):
        self.count = 0
        self.request = request
        self.offset = offset
        self.limit = limit

    @abstractmethod
    async def get_count(self):
        """Get total count query."""
        pass  # pragma: no cover

    def get_next_url(self) -> Optional[str]:
        """Constructs `next` parameter in resulting JSON."""
        if self.offset + self.limit >= self.count:
            return None
        return str(
            self.request.url.include_query_params(
                limit=self.limit, offset=self.offset + self.limit
            )
        )

    def get_previous_url(self) -> Optional[str]:
        """Constructs `previous` parameter in resulting JSON."""
        if self.offset <= 0:
            return None

        if self.offset - self.limit <= 0:
            return str(self.request.url.remove_query_params(keys=["offset"]))

        return str(
            self.request.url.include_query_params(
                limit=self.limit, offset=self.offset - self.limit
            )
        )

    @abstractmethod
    async def get_results(self):
        """Get results according to offset and limit parameters."""
        pass  # pragma: no cover

    async def query(self, *args, **kwargs):
        """Paginate and returns the results."""
        self.count = await self.get_count()
        results = await self.get_results(*args, **kwargs)
        return {
            "count": self.count,
            "next": self.get_next_url(),
            "previous": self.get_previous_url(),
            "results": results,
        }


class ListPagination(Pagination):
    """Format list output paginated."""

    def __init__(self, output, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output = output

    async def get_count(self):
        """List count."""
        return len(self.output)

    async def get_results(self, *args, **kwargs):
        """List results."""
        return self.output[self.offset : self.offset + self.limit]  # noqa: E203


class DatabasePagination(Pagination):
    def __init__(self, database, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.database = database

    async def get_count(self):
        """Database count."""
        return await self.database.all_count()

    async def get_results(self, *args, **kwargs):
        """Database results."""
        return await self.database.all(
            *args, offset=self.offset, limit=self.limit, **kwargs
        )
