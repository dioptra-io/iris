"""Results operations."""
from ipaddress import ip_network
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import IPvAnyAddress, IPvAnyNetwork

from iris.api.dependencies import get_database
from iris.api.pagination import DatabasePagination
from iris.api.security import get_current_active_user
from iris.commons.database import (
    Database,
    Interfaces,
    Links,
    Prefixes,
    Replies,
    agents,
    measurements,
)
from iris.commons.database.results import QueryWrapper
from iris.commons.schemas import public

router = APIRouter()


async def get_results(
    request,
    measurement_uuid: UUID,
    agent_uuid: UUID,
    offset: int,
    limit: int,
    user: public.Profile,
    wrapper: QueryWrapper,
):
    measurement_info = await measurements.get(
        wrapper.database, user.username, measurement_uuid
    )
    if measurement_info is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Measurement not found"
        )

    agent_info = await agents.get(wrapper.database, measurement_uuid, agent_uuid)

    if agent_info is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"The agent `{agent_uuid}` "
                f"did not participate to measurement `{measurement_uuid}`"
            ),
        )

    if agent_info.state != public.MeasurementState.Finished:
        raise HTTPException(
            status_code=status.HTTP_412_PRECONDITION_FAILED,
            detail=(
                f"The agent `{agent_uuid}` "
                f"has not finished the measurement `{measurement_uuid}`"
            ),
        )

    is_table_exists = await wrapper.exists()
    if not is_table_exists:
        return {"count": 0, "next": None, "previous": None, "results": []}

    querier = DatabasePagination(
        wrapper,
        wrapper.__class__.all,
        wrapper.__class__.all_count,
        request,
        offset,
        limit,
    )
    return await querier.query()


@router.get(
    "/{measurement_uuid}/{agent_uuid}/prefixes",
    response_model=public.Paginated[public.Prefix],
    responses={404: {"model": public.GenericException}},
    summary="Get measurement prefixes.",
)
async def get_prefixes_results(
    request: Request,
    measurement_uuid: UUID,
    agent_uuid: UUID,
    contains_network: Optional[IPvAnyNetwork] = Query(None),
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=0, le=200),
    user: public.Profile = Depends(get_current_active_user),
    database: Database = Depends(get_database),
):
    """Get replies results."""
    wrapper = Prefixes(
        database, measurement_uuid, agent_uuid, reply_src_addr_in=contains_network
    )
    return await get_results(
        request, measurement_uuid, agent_uuid, offset, limit, user, wrapper
    )


@router.get(
    "/{measurement_uuid}/{agent_uuid}/replies/{prefix}",
    response_model=public.Paginated[public.Reply],
    responses={404: {"model": public.GenericException}},
    summary="Get measurement replies.",
)
async def get_replies_results(
    request: Request,
    measurement_uuid: UUID,
    agent_uuid: UUID,
    prefix: IPvAnyAddress,
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=0, le=200),
    user: public.Profile = Depends(get_current_active_user),
    database: Database = Depends(get_database),
):
    """Get replies results."""
    wrapper = Replies(database, measurement_uuid, agent_uuid, subset=ip_network(prefix))
    return await get_results(
        request, measurement_uuid, agent_uuid, offset, limit, user, wrapper
    )


@router.get(
    "/{measurement_uuid}/{agent_uuid}/interfaces/{prefix}",
    response_model=public.Paginated[public.Interface],
    responses={404: {"model": public.GenericException}},
    summary="Get measurement interfaces.",
)
async def get_interfaces_results(
    request: Request,
    measurement_uuid: UUID,
    agent_uuid: UUID,
    prefix: IPvAnyAddress,
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=0, le=200),
    user: public.Profile = Depends(get_current_active_user),
    database: Database = Depends(get_database),
):
    """Get interfaces results."""
    wrapper = Interfaces(
        database, measurement_uuid, agent_uuid, subset=ip_network(prefix)
    )
    return await get_results(
        request, measurement_uuid, agent_uuid, offset, limit, user, wrapper
    )


@router.get(
    "/{measurement_uuid}/{agent_uuid}/links/by-prefix/{prefix}",
    response_model=public.Paginated[public.Link],
    responses={404: {"model": public.GenericException}},
    summary="Get measurement links.",
)
async def get_links_results_by_prefix(
    request: Request,
    measurement_uuid: UUID,
    agent_uuid: UUID,
    prefix: IPvAnyAddress,
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=0, le=200),
    user: public.Profile = Depends(get_current_active_user),
    database: Database = Depends(get_database),
):
    """Get links results."""
    wrapper = Links(database, measurement_uuid, agent_uuid, subset=ip_network(prefix))
    return await get_results(
        request, measurement_uuid, agent_uuid, offset, limit, user, wrapper
    )


@router.get(
    "/{measurement_uuid}/{agent_uuid}/links/by-adjacency/{address}",
    response_model=public.Paginated[public.Link],
    responses={404: {"model": public.GenericException}},
    summary="Get measurement links.",
)
async def get_links_results_by_adjacency(
    request: Request,
    measurement_uuid: UUID,
    agent_uuid: UUID,
    address: IPvAnyAddress,
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=0, le=200),
    user: public.Profile = Depends(get_current_active_user),
    database: Database = Depends(get_database),
):
    """Get links results."""
    wrapper = Links(database, measurement_uuid, agent_uuid, near_or_far_addr=address)
    return await get_results(
        request, measurement_uuid, agent_uuid, offset, limit, user, wrapper
    )
