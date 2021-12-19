"""
The Agents database stores the status of each agents and their measurements.
"""
import json
from datetime import datetime
from typing import List, Optional
from uuid import UUID

from iris.commons.database.database import Database
from iris.commons.schemas import private, public


def table(database: Database) -> str:
    return database.settings.TABLE_NAME_AGENTS


def formatter(row: dict) -> public.MeasurementAgent:
    return public.MeasurementAgent(
        uuid=row["agent_uuid"],
        state=public.MeasurementState(row["state"]),
        specific=public.MeasurementAgentSpecific(
            target_file=row["target_file"],
            target_file_content=[],
            probing_rate=row["probing_rate"],
            tool_parameters=public.ToolParameters.parse_raw(row["tool_parameters"]),
        ),
        parameters=public.AgentParameters.parse_raw(row["agent_parameters"]),
        probing_statistics=[
            public.ProbingStatistics(**x) for x in json.loads(row["probing_statistics"])
        ],
    )


async def create_table(database: Database, drop: bool = False) -> None:
    if drop:
        await database.call(
            "DROP TABLE IF EXISTS {table:Identifier}", params={"table": table(database)}
        )

    await database.call(
        """
        CREATE TABLE IF NOT EXISTS {table:Identifier}
        (
            measurement_uuid   UUID,
            agent_uuid         UUID,
            target_file        String,
            probing_rate       Nullable(UInt32),
            probing_statistics String,
            agent_parameters   String,
            tool_parameters    String,
            state              Enum8('ongoing' = 1, 'finished' = 2, 'canceled' = 3),
            timestamp          DateTime
        )
        ENGINE MergeTree
        ORDER BY (measurement_uuid, agent_uuid)
        """,
        params={"table": table(database)},
    )


async def all(
    database: Database, measurement_uuid: UUID
) -> List[public.MeasurementAgent]:
    """Get all measurement information."""
    responses = await database.call(
        """
        SELECT *
        FROM {table:Identifier}
        WHERE measurement_uuid={uuid:UUID}
        """,
        params={"table": table(database), "uuid": measurement_uuid},
    )
    return [formatter(response) for response in responses]


async def get(
    database: Database, measurement_uuid: UUID, agent_uuid: UUID
) -> Optional[public.MeasurementAgent]:
    """Get measurement information about a agent."""
    responses = await database.call(
        """
        SELECT *
        FROM {table:Identifier}
        WHERE measurement_uuid={measurement_uuid:UUID}
        AND agent_uuid={agent_uuid:UUID}
        """,
        params={
            "table": table(database),
            "measurement_uuid": measurement_uuid,
            "agent_uuid": agent_uuid,
        },
    )
    if responses:
        return formatter(responses[0])
    return None


async def register(
    database: Database,
    measurement_request: private.MeasurementRequest,
    agent_uuid: UUID,
    agent_parameters: public.AgentParameters,
) -> None:
    agent = measurement_request.agent(agent_uuid)
    await database.call(
        "INSERT INTO {table:Identifier} FORMAT JSONEachRow",
        params={"table": table(database)},
        values=[
            {
                "measurement_uuid": measurement_request.uuid,
                "agent_uuid": agent.uuid,
                "target_file": agent.target_file,
                "probing_rate": agent.probing_rate,
                "probing_statistics": json.dumps([]),
                "agent_parameters": agent_parameters.json(),
                "tool_parameters": agent.tool_parameters.json(),
                "state": public.MeasurementState.Ongoing.value,
                "timestamp": datetime.utcnow().isoformat(timespec="seconds"),
            }
        ],
    )


async def store_probing_statistics(
    database: Database,
    measurement_uuid: UUID,
    agent_uuid: UUID,
    probing_statistics: public.ProbingStatistics,
) -> None:
    # Get the probing statistics already stored
    measurement = await get(database, measurement_uuid, agent_uuid)
    assert measurement

    # Update the probing statistics
    current_probing_statistics = {
        x.round.encode(): x for x in measurement.probing_statistics
    }
    current_probing_statistics[probing_statistics.round.encode()] = probing_statistics

    # Store the updated statistics on the database
    await database.call(
        """
        ALTER TABLE {table:Identifier}
        UPDATE probing_statistics={probing_statistics:String}
        WHERE measurement_uuid={measurement_uuid:UUID}
        AND agent_uuid={agent_uuid:UUID}
        SETTINGS mutations_sync=1
        """,
        params={
            "table": table(database),
            "probing_statistics": json.dumps(
                [json.loads(x.json()) for x in current_probing_statistics.values()]
            ),
            "measurement_uuid": measurement_uuid,
            "agent_uuid": agent_uuid,
        },
    )


async def stamp_finished(
    database: Database, measurement_uuid: UUID, agent_uuid: UUID
) -> None:
    await database.call(
        """
        ALTER TABLE {table:Identifier}
        UPDATE state={state:String}
        WHERE measurement_uuid={measurement_uuid:UUID}
        AND agent_uuid={agent_uuid:UUID}
        SETTINGS mutations_sync=1
        """,
        params={
            "table": table(database),
            "state": public.MeasurementState.Finished.value,
            "measurement_uuid": measurement_uuid,
            "agent_uuid": agent_uuid,
        },
    )


async def stamp_canceled(
    database: Database, measurement_uuid: UUID, agent_uuid: UUID
) -> None:
    await database.call(
        """
        ALTER TABLE {table:Identifier}
        UPDATE state={state:String}
        WHERE measurement_uuid={measurement_uuid:UUID}
        AND agent_uuid={agent_uuid:UUID}
        SETTINGS mutations_sync=1
        """,
        params={
            "table": table(database),
            "state": public.MeasurementState.Canceled.value,
            "measurement_uuid": measurement_uuid,
            "agent_uuid": agent_uuid,
        },
    )
