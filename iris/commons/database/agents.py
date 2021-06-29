import json
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional
from uuid import UUID

from iris.commons.database.database import Database
from iris.commons.dataclasses import ParametersDataclass


@dataclass(frozen=True)
class Agents(Database):
    """
    The Agents database stores the status of each agents and their measurements.
    """

    @property
    def table(self) -> str:
        return self.settings.TABLE_NAME_AGENTS

    async def create_table(self, drop: bool = False) -> None:
        if drop:
            await self.call(f"DROP TABLE IF EXISTS {self.table}")

        await self.call(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table}
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
        )

    async def all(self, measurement_uuid: UUID) -> List[dict]:
        """Get all measurement information."""
        responses = await self.call(
            f"SELECT * FROM {self.table} WHERE measurement_uuid=%(uuid)s",
            {"uuid": measurement_uuid},
        )
        return [self.formatter(response) for response in responses]

    async def get(self, measurement_uuid: UUID, agent_uuid: UUID) -> Optional[dict]:
        """Get measurement information about a agent."""
        responses = await self.call(
            f"SELECT * FROM {self.table} "
            "WHERE measurement_uuid=%(measurement_uuid)s "
            "AND agent_uuid=%(agent_uuid)s",
            {"measurement_uuid": measurement_uuid, "agent_uuid": agent_uuid},
        )
        if responses:
            return self.formatter(responses[0])
        return None

    async def register(self, parameters: ParametersDataclass) -> None:
        await self.call(
            f"INSERT INTO {self.table} VALUES",
            [
                {
                    "measurement_uuid": parameters.measurement_uuid,
                    "agent_uuid": parameters.agent_uuid,
                    "target_file": parameters.target_file,
                    "probing_rate": parameters.probing_rate,
                    "probing_statistics": json.dumps({}),
                    "agent_parameters": json.dumps(parameters.agent_parameters),
                    "tool_parameters": json.dumps(parameters.tool_parameters),
                    "state": "ongoing",
                    "timestamp": datetime.now(),
                }
            ],
        )

    async def store_probing_statistics(
        self,
        measurement_uuid: UUID,
        agent_uuid: UUID,
        round_number: str,
        probing_statistics: dict,
    ) -> None:
        # Get the probing statistics already stored
        current_probing_statistics = (await self.get(measurement_uuid, agent_uuid))[
            "probing_statistics"
        ]

        # Update the probing statistics
        current_probing_statistics[round_number] = probing_statistics

        # Store the updated statistics on the database
        await self.call(
            f"""
            ALTER TABLE {self.table}
            UPDATE probing_statistics=%(probing_statistics)s
            WHERE measurement_uuid=%(measurement_uuid)s
            AND agent_uuid=%(agent_uuid)s
            SETTINGS mutations_sync=1
            """,
            {
                "probing_statistics": json.dumps(current_probing_statistics),
                "measurement_uuid": measurement_uuid,
                "agent_uuid": agent_uuid,
            },
        )

    async def stamp_finished(self, measurement_uuid: UUID, agent_uuid: UUID) -> None:
        await self.call(
            f"""
            ALTER TABLE {self.table}
            UPDATE state=%(state)s
            WHERE measurement_uuid=%(measurement_uuid)s
            AND agent_uuid=%(agent_uuid)s
            SETTINGS mutations_sync=1
            """,
            {
                "state": "finished",
                "measurement_uuid": measurement_uuid,
                "agent_uuid": agent_uuid,
            },
        )

    async def stamp_canceled(self, measurement_uuid: UUID, agent_uuid: UUID) -> None:
        await self.call(
            f"""
            ALTER TABLE {self.table}
            UPDATE state=%(state)s
            WHERE measurement_uuid=%(measurement_uuid)s
            AND agent_uuid=%(agent_uuid)s
            SETTINGS mutations_sync=1
            """,
            {
                "state": "canceled",
                "measurement_uuid": measurement_uuid,
                "agent_uuid": agent_uuid,
            },
        )

    @staticmethod
    def formatter(row: tuple) -> dict:
        return {
            "uuid": str(row[1]),
            "target_file": row[2],
            "probing_rate": row[3],
            "probing_statistics": json.loads(row[4]),
            "agent_parameters": json.loads(row[5]),
            "tool_parameters": json.loads(row[6]),
            "state": row[7],
        }
