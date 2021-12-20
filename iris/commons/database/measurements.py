"""Interface that handle measurements history."""
from datetime import datetime
from typing import List, Optional
from uuid import UUID

from sqlmodel import Session, select

from iris.commons.database.agents import formatter
from iris.commons.database.database import Database
from iris.commons.schemas.measurements import (
    Measurement,
    MeasurementRequest,
    MeasurementState,
)
from iris.commons.schemas.measurements2 import Measurement2


# TODO: Merge
def formatter(measurement: Measurement2) -> Measurement:
    return Measurement(
        uuid=measurement.uuid,
        user_id=measurement.user_id,
        tool=measurement.tool,
        tags=measurement.tags,
        state=measurement.state,
        start_time=measurement.start_time,
        end_time=measurement.end_time,
        agents=[],
    )


async def all_count(
    database: Database, user_id: Optional[UUID] = None, tag: Optional[str] = None
) -> int:
    """Get the count of all results."""
    with Session(database.settings.sqlalchemy_engine()) as session:
        # TODO: Proper tag and measurement_tag tables and query
        measurements = session.exec(select(Measurement2)).all()
        kept = []
        for measurement in measurements:
            if user_id and measurement.user_id != user_id:
                continue
            if tag and tag not in measurement.tags:
                continue
            kept.append(measurement)
        return len(kept)


async def all(
    database: Database,
    offset: int,
    limit: int,
    user_id: Optional[UUID] = None,
    tag: Optional[str] = None,
) -> List[Measurement]:
    """Get all measurements uuid for a given user or a tag."""
    with Session(database.settings.sqlalchemy_engine()) as session:
        # TODO: Proper tag and measurement_tag tables and query
        measurements = session.exec(
            select(Measurement2).offset(offset).limit(limit)
        ).all()
        kept = []
        for measurement in measurements:
            if user_id and measurement.user_id != user_id:
                continue
            if tag and tag not in measurement.tags:
                continue
            kept.append(measurement)
    return [formatter(measurement) for measurement in kept]


async def get(database: Database, uuid: UUID) -> Optional[Measurement]:
    """Get a measurement information based on its uuid for a given user of a tag."""
    with Session(database.settings.sqlalchemy_engine()) as session:
        measurement = session.get(Measurement2, uuid)
    if not measurement:
        return None
    return formatter(measurement)


async def register(
    database: Database,
    measurement_request: MeasurementRequest,
) -> None:
    """Register a measurement."""
    measurement = Measurement2(
        uuid=measurement_request.uuid,
        user_id=measurement_request.user_id,
        tool=measurement_request.tool,
        tags=measurement_request.tags,
        state=MeasurementState.Ongoing,
        start_time=measurement_request.start_time,
        end_time=None,
    )
    with Session(database.settings.sqlalchemy_engine()) as session:
        session.add(measurement)
        session.commit()


async def set_state(database: Database, uuid: UUID, state: MeasurementState) -> None:
    """Set the state of a measurement."""
    with Session(database.settings.sqlalchemy_engine()) as session:
        measurement = session.get(Measurement2, uuid)
        measurement.state = state
        session.add(measurement)
        session.commit()


async def set_end_time(database: Database, uuid: UUID) -> None:
    """Stamp the end time for a measurement."""
    with Session(database.settings.sqlalchemy_engine()) as session:
        measurement = session.get(Measurement2, uuid)
        measurement.end_time = datetime.utcnow()
        session.add(measurement)
        session.commit()
