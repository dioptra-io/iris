import json
from dataclasses import dataclass
from logging import LoggerAdapter
from typing import Any, List, Optional

import httpx
from diamond_miner.queries import Query
from httpx import HTTPStatusError

from iris.commons.settings import CommonSettings, fault_tolerant


class QueryError(Exception):
    pass


@dataclass(frozen=True)
class ClickHouse:
    settings: CommonSettings
    logger: LoggerAdapter

    @fault_tolerant(CommonSettings.database_retry)
    async def call(
        self,
        query: str,
        *,
        database: Optional[str] = None,
        params: Optional[dict] = None,
        values: Optional[list] = None,
        timeout=(1, 60),
    ) -> List[dict]:
        # TODO: Cleanup this code and move to a dedicated package?
        query_params = {}
        content = ""

        if params:
            query_params = {f"param_{k}": v for k, v in params.items()}

        if values:
            for value in values:
                content += json.dumps(value, default=str) + "\n"

        params_ = {
            "default_format": "JSONEachRow",
            "query": query,
            **query_params,
        }

        if database:
            params_["database"] = database

        async with httpx.AsyncClient() as client:
            r = await client.post(
                url=self.settings.CLICKHOUSE_URL,
                content=content,
                params=params_,
                timeout=timeout,
            )
            try:
                r.raise_for_status()
                if text := r.text.strip():
                    return [json.loads(line) for line in text.split("\n")]
                return []
            except HTTPStatusError as e:
                raise QueryError(r.content) from e

    @fault_tolerant(CommonSettings.database_retry)
    async def execute(self, query: Query, measurement_id: str, **kwargs: Any):
        return query.execute(self.settings.CLICKHOUSE_URL, measurement_id, **kwargs)

    async def grant_public_access(self, table) -> None:
        if public_user := self.settings.CLICKHOUSE_PUBLIC_USER:
            await self.call(
                "GRANT SELECT ON {table:Identifier} TO {user:Identifier}",
                params={"table": table, "user": public_user},
            )
