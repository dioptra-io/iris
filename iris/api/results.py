"""Results operations."""
from ipaddress import ip_network
from typing import Dict, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import IPvAnyAddress, IPvAnyNetwork

from iris.api import schemas
from iris.api.pagination import DatabasePagination
from iris.api.security import get_current_active_user
from iris.commons.database import (
    Agents,
    Interfaces,
    Links,
    Measurements,
    Prefixes,
    Replies,
)

router = APIRouter()


async def get_results(
    request, measurement_uuid, agent_uuid, offset, limit, user, database
):
    measurement_info = await Measurements(request.app.settings, request.app.logger).get(
        user["username"], measurement_uuid
    )
    if measurement_info is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Measurement not found"
        )

    agent_info = await Agents(request.app.settings, request.app.logger).get(
        measurement_uuid, agent_uuid
    )

    if agent_info is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"The agent `{agent_uuid}` "
                f"did not participate to measurement `{measurement_uuid}`"
            ),
        )

    if agent_info["state"] != "finished":
        raise HTTPException(
            status_code=status.HTTP_412_PRECONDITION_FAILED,
            detail=(
                f"The agent `{agent_uuid}` "
                f"has not finished the measurement `{measurement_uuid}`"
            ),
        )

    is_table_exists = await database.exists()
    if not is_table_exists:
        return {"count": 0, "next": None, "previous": None, "results": []}

    querier = DatabasePagination(database, request, offset, limit)
    return await querier.query()


@router.get(
    "/{measurement_uuid}/{agent_uuid}/prefixes",
    response_model=schemas.Prefixes,
    responses={404: {"model": schemas.GenericException}},
    summary="Get measurement prefixes.",
)
async def get_prefixes_results(
    request: Request,
    measurement_uuid: UUID,
    agent_uuid: UUID,
    contains_network: Optional[IPvAnyNetwork] = Query(None),
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=0, le=200),
    user: Dict = Depends(get_current_active_user),
):
    """Get replies results."""
    database = Prefixes(
        request.app.settings,
        request.app.logger,
        measurement_uuid,
        agent_uuid,
        reply_src_addr_in=contains_network,
    )
    return await get_results(
        request, measurement_uuid, agent_uuid, offset, limit, user, database
    )


@router.get(
    "/{measurement_uuid}/{agent_uuid}/replies/{prefix}",
    response_model=schemas.Replies,
    responses={404: {"model": schemas.GenericException}},
    summary="Get measurement replies.",
)
async def get_replies_results(
    request: Request,
    measurement_uuid: UUID,
    agent_uuid: UUID,
    prefix: IPvAnyAddress,
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=0, le=200),
    user: Dict = Depends(get_current_active_user),
):
    """Get replies results."""
    database = Replies(
        request.app.settings,
        request.app.logger,
        measurement_uuid,
        agent_uuid,
        subset=ip_network(prefix),
    )
    return await get_results(
        request, measurement_uuid, agent_uuid, offset, limit, user, database
    )


@router.get(
    "/{measurement_uuid}/{agent_uuid}/interfaces/{prefix}",
    response_model=schemas.Interfaces,
    responses={404: {"model": schemas.GenericException}},
    summary="Get measurement interfaces.",
)
async def get_interfaces_results(
    request: Request,
    measurement_uuid: UUID,
    agent_uuid: UUID,
    prefix: IPvAnyAddress,
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=0, le=200),
    user: Dict = Depends(get_current_active_user),
):
    """Get interfaces results."""
    database = Interfaces(
        request.app.settings,
        request.app.logger,
        measurement_uuid,
        agent_uuid,
        subset=ip_network(prefix),
    )
    return await get_results(
        request, measurement_uuid, agent_uuid, offset, limit, user, database
    )


@router.get(
    "/{measurement_uuid}/{agent_uuid}/links/{prefix}",
    response_model=schemas.Links,
    responses={404: {"model": schemas.GenericException}},
    summary="Get measurement links.",
)
async def get_links_results(
    request: Request,
    measurement_uuid: UUID,
    agent_uuid: UUID,
    prefix: IPvAnyAddress,
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=0, le=200),
    user: Dict = Depends(get_current_active_user),
):
    """Get links results."""
    database = Links(
        request.app.settings,
        request.app.logger,
        measurement_uuid,
        agent_uuid,
        subset=ip_network(prefix),
    )
    return await get_results(
        request, measurement_uuid, agent_uuid, offset, limit, user, database
    )
