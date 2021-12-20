"""Public Measurements operations."""

from typing import List
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from iris.api.authentication import current_verified_user
from iris.api.dependencies import get_database, get_redis, get_storage
from iris.api.pagination import DatabasePagination
from iris.commons.database import Database, agents, measurements
from iris.commons.redis import Redis
from iris.commons.schemas.exceptions import GenericException
from iris.commons.schemas.measurements import (
    Measurement,
    MeasurementState,
    MeasurementSummary,
)
from iris.commons.schemas.paging import Paginated
from iris.commons.schemas.users import UserDB
from iris.commons.storage import Storage

router = APIRouter()


@router.get(
    "/public/",
    response_model=Paginated[MeasurementSummary],
    summary="Get all public measurements.",
)
async def get_measurements_public(
    request: Request,
    offset: int = Query(0, ge=0),
    limit: int = Query(20, ge=0, le=200),
    user: UserDB = Depends(current_verified_user),
    database: Database = Depends(get_database),
    redis: Redis = Depends(get_redis),
):
    """Get all measurements."""
    querier = DatabasePagination(
        database, measurements.all, measurements.all_count, request, offset, limit
    )
    output = await querier.query(tag="public")

    measurements_: List[Measurement] = output["results"]
    summaries: List[MeasurementSummary] = []

    for measurement in measurements_:
        state = await redis.get_measurement_state(measurement.uuid)
        if not state or state == MeasurementState.Unknown:
            state = measurement.state
        summaries.append(
            MeasurementSummary(
                uuid=measurement.uuid,
                state=state,
                tool=measurement.tool,
                tags=measurement.tags,
                start_time=measurement.start_time,
                end_time=measurement.end_time,
            )
        )

    output["results"] = summaries

    return output


@router.get(
    "/public/{measurement_uuid}",
    response_model=Measurement,
    responses={404: {"model": GenericException}},
    summary="Get public measurement specified by UUID.",
)
async def get_measurement_by_uuid(
    request: Request,
    measurement_uuid: UUID,
    user: UserDB = Depends(current_verified_user),
    database: Database = Depends(get_database),
    redis: Redis = Depends(get_redis),
    storage: Storage = Depends(get_storage),
):
    """Get measurement information by uuid."""
    measurement = await measurements.get(database, measurement_uuid, tag="public")
    if measurement is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Measurement not found"
        )

    state = await redis.get_measurement_state(measurement_uuid)
    if state and state != MeasurementState.Unknown:
        measurement = measurement.copy(update={"state": state})

    measurement_agents = []
    agents_info = await agents.all(database, measurement.uuid)

    for agent_info in agents_info:
        if measurement.state == MeasurementState.Waiting:
            agent_info = agent_info.copy(update={"state": measurement.state})
        try:
            target_file = await storage.get_file_no_retry(
                storage.archive_bucket(user.id),
                f"targets__{measurement.uuid}__{agent_info.uuid}.csv",
            )
            target_file_content = [c.strip() for c in target_file["content"].split()]
            # NOTE: Don't display the measurement if the file is too big
            # to avoid to slow down the API.
            if len(target_file_content) <= 100:
                agent_info = agent_info.copy(
                    update={
                        "specific": agent_info.specific.copy(
                            update={"target_file_content": target_file_content}
                        )
                    }
                )
        except Exception:
            pass
        measurement_agents.append(agent_info)

    return measurement.copy(update={"agents": measurement_agents})
