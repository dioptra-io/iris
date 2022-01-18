#!/usr/bin/env python3
# Migrate Iris < 1.0 database to Iris 1.0.
import asyncio
import datetime
import os
from json import JSONDecodeError

import httpx
from sqlalchemy import select

from iris.commons.dependencies import (
    get_engine_context,
    get_session_context,
    get_settings,
)
from iris.commons.models import (
    AgentParameters,
    FlowMapper,
    Measurement,
    MeasurementAgent,
    MeasurementAgentState,
    Round,
    Tool,
    ToolParameters,
    UserTable,
)

MIGRATION_TAG = os.environ.get("IRIS_MIGRATION_TAG", "migrated-pre-1.0")
MIGRATION_USER = os.environ.get("IRIS_MIGRATION_USER", "admin@example.org")


class Client:
    def __init__(self, base_url, username, password):
        self.base_url = base_url
        self.username = username
        self.password = password

    async def __aenter__(self):
        self.client = httpx.AsyncClient(
            base_url=self.base_url,
            follow_redirects=True,
            timeout=30,
        )
        response = await self.client.post(
            "/profile/token",
            data=dict(
                username=self.username,
                password=self.password,
            ),
        )
        data = response.json()
        self.client.headers = dict(Authorization=f"Bearer {data['access_token']}")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()

    async def fetch_measurements(self):
        results = []
        next_url = "/measurements/?limit=200"
        while next_url:
            response = await self.client.get(next_url)
            data = response.json()
            results.extend(data["results"])
            next_url = data["next"]
            # Workaround for the pagination bug of Iris < 1.0
            if next_url and self.base_url.startswith("https://"):
                next_url = next_url.replace("http://", "https://")
        return results

    async def fetch_measurement(self, uuid):
        response = await self.client.get(f"/measurements/{uuid}")
        try:
            return response.json()
        except JSONDecodeError as e:
            raise RuntimeError(response.content) from e


async def main():
    settings = get_settings()
    new_measurements = []

    with get_engine_context(settings) as engine:
        with get_session_context(engine) as session:
            user_id = str(
                session.exec(
                    select(UserTable.id).where(UserTable.email == MIGRATION_USER)
                ).one()[0]
            )

    async with Client(
        os.environ["IRIS_URL"],
        os.environ["IRIS_USERNAME"],
        os.environ["IRIS_PASSWORD"],
    ) as client:
        measurements = await client.fetch_measurements()
        tasks = [
            asyncio.create_task(client.fetch_measurement(measurement["uuid"]))
            for measurement in measurements
        ]
        for i, task in enumerate(asyncio.as_completed(tasks)):
            print(f"{i}/{len(tasks)}")
            measurement = await task
            new_mas = []
            for ma in measurement["agents"]:
                specific = ma["specific"]
                agent_parameters = ma["parameters"]
                tool_parameters = specific["tool_parameters"]
                probing_statistics = ma["probing_statistics"]
                new_ma = MeasurementAgent(
                    probing_rate=specific["probing_rate"],
                    target_file=specific["target_file"],
                    state=MeasurementAgentState(
                        ma["state"].replace("waiting", "created")
                    ),
                    agent_uuid=ma["uuid"],
                    agent_parameters=AgentParameters(
                        version=agent_parameters["version"],
                        hostname=agent_parameters["hostname"],
                        ipv4_address=agent_parameters["ipv4_address"],
                        ipv6_address=agent_parameters["ipv6_address"],
                        min_ttl=agent_parameters["min_ttl"],
                        max_probing_rate=agent_parameters["max_probing_rate"],
                        tags=agent_parameters["agent_tags"],
                    ),
                    tool_parameters=ToolParameters(
                        initial_source_port=tool_parameters["initial_source_port"],
                        destination_port=tool_parameters["destination_port"],
                        max_round=tool_parameters["max_round"],
                        flow_mapper=FlowMapper(tool_parameters["flow_mapper"]),
                        flow_mapper_kwargs=tool_parameters["flow_mapper_kwargs"],
                        prefix_len_v4=tool_parameters["prefix_len_v4"],
                        prefix_len_v6=tool_parameters["prefix_len_v6"],
                        global_min_ttl=tool_parameters["global_min_ttl"],
                        global_max_ttl=tool_parameters["global_max_ttl"],
                    ),
                )
                new_ma.probing_statistics = {}
                for ps in probing_statistics:
                    round_ = Round(
                        number=ps["round"]["number"],
                        limit=ps["round"]["limit"],
                        offset=ps["round"]["offset"],
                    )
                    new_ma.probing_statistics[round_.encode()] = ps
                if new_ma.probing_statistics.values():
                    new_ma.start_time = min(
                        datetime.datetime.fromisoformat(ps["start_time"])
                        for ps in new_ma.probing_statistics.values()
                    )
                    new_ma.end_time = max(
                        datetime.datetime.fromisoformat(ps["end_time"])
                        for ps in new_ma.probing_statistics.values()
                    )
                else:
                    new_ma.start_time = None
                    new_ma.end_time = None
                new_mas.append(new_ma)
            new_measurement = Measurement(
                uuid=measurement["uuid"],
                tool=Tool(measurement["tool"]),
                tags=measurement["tags"] + [MIGRATION_TAG],
                creation_time=datetime.datetime.fromisoformat(
                    measurement["start_time"]
                ),
                user_id=user_id,
                agents=new_mas,
            )
            for new_ma in new_measurement.agents:
                if new_ma.start_time == datetime.datetime(1970, 1, 1, 0, 0, 0):
                    new_ma.start_time = None
                if new_ma.end_time == datetime.datetime(1970, 1, 1, 0, 0, 0):
                    new_ma.end_time = None
                if not new_ma.start_time and measurement["start_time"]:
                    new_ma.start_time = datetime.datetime.fromisoformat(
                        measurement["start_time"]
                    )
                if not new_ma.end_time and measurement["end_time"]:
                    new_ma.end_time = datetime.datetime.fromisoformat(
                        measurement["end_time"]
                    )
            new_measurements.append(new_measurement)

    with get_engine_context(settings) as engine:
        with get_session_context(engine) as session:
            session.add_all(new_measurements)
            session.commit()


if __name__ == "__main__":
    asyncio.run(main())
