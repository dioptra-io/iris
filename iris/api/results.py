"""Results operations."""

from typing import Dict
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from iris.api.pagination import DatabasePagination
from iris.api.schemas import (
    ExceptionResponse,
    InterfacesResultsResponse,
    LinksResultsResponse,
    RepliesResultsResponse,
)
from iris.api.security import get_current_active_user
from iris.commons.database import (
    Agents,
    GetInterfacesResults,
    GetLinksResults,
    GetPrefixesResults,
    GetReplyResults,
    Measurements,
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
    response_model=RepliesResultsResponse,
    responses={404: {"model": ExceptionResponse}},
    summary="Get prefixes results.",
)
async def get_prefixes_results(
    request: Request,
    measurement_uuid: UUID,
    agent_uuid: UUID,
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=0, le=200),
    user: Dict = Depends(get_current_active_user),
):
    """Get replies results."""
    database = GetPrefixesResults(
        request.app.settings, request.app.logger, measurement_uuid, agent_uuid
    )
    return await get_results(
        request, measurement_uuid, agent_uuid, offset, limit, user, database
    )


@router.get(
    "/{measurement_uuid}/{agent_uuid}/replies",
    response_model=RepliesResultsResponse,
    responses={404: {"model": ExceptionResponse}},
    summary="Get replies results.",
)
async def get_replies_results(
    request: Request,
    measurement_uuid: UUID,
    agent_uuid: UUID,
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=0, le=200),
    user: Dict = Depends(get_current_active_user),
):
    """Get replies results."""
    database = GetReplyResults(
        request.app.settings, request.app.logger, measurement_uuid, agent_uuid
    )
    return await get_results(
        request, measurement_uuid, agent_uuid, offset, limit, user, database
    )


@router.get(
    "/{measurement_uuid}/{agent_uuid}/interfaces",
    response_model=InterfacesResultsResponse,
    responses={404: {"model": ExceptionResponse}},
    summary="Get interfaces results.",
)
async def get_interfaces_results(
    request: Request,
    measurement_uuid: UUID,
    agent_uuid: UUID,
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=0, le=200),
    user: Dict = Depends(get_current_active_user),
):
    """Get interfaces results."""
    database = GetInterfacesResults(
        request.app.settings, request.app.logger, measurement_uuid, agent_uuid
    )
    return await get_results(
        request, measurement_uuid, agent_uuid, offset, limit, user, database
    )


@router.get(
    "/{measurement_uuid}/{agent_uuid}/links",
    response_model=LinksResultsResponse,
    responses={404: {"model": ExceptionResponse}},
    summary="Get links results.",
)
async def get_links_results(
    request: Request,
    measurement_uuid: UUID,
    agent_uuid: UUID,
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=0, le=200),
    user: Dict = Depends(get_current_active_user),
):
    """Get links results."""
    database = GetLinksResults(
        request.app.settings, request.app.logger, measurement_uuid, agent_uuid
    )
    return await get_results(
        request, measurement_uuid, agent_uuid, offset, limit, user, database
    )
