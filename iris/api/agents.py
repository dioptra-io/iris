"""agents operations."""

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from iris.api.authentication import current_verified_user
from iris.api.dependencies import get_redis
from iris.api.pagination import ListPagination
from iris.commons.redis import Redis
from iris.commons.schemas.agents import Agent
from iris.commons.schemas.exceptions import GenericException
from iris.commons.schemas.paging import Paginated
from iris.commons.schemas.users import UserDB

router = APIRouter()


@router.get(
    "/",
    response_model=Paginated[Agent],
    summary="Get all agents.",
)
async def get_agents(
    request: Request,
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=0, le=200),
    user: UserDB = Depends(current_verified_user),
    redis: Redis = Depends(get_redis),
):
    """Get all agents."""
    # First check is user has probing enabled
    if not user.probing_enabled:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You must have probing enabled to access this resource",
        )

    agents = await redis.get_agents()
    querier = ListPagination(agents, request, offset, limit)
    return await querier.query()


@router.get(
    "/{uuid}",
    response_model=Agent,
    responses={404: {"model": GenericException}},
    summary="Get agent specified by UUID.",
)
async def get_agent_by_uuid(
    request: Request,
    uuid: UUID,
    user: UserDB = Depends(current_verified_user),
    redis: Redis = Depends(get_redis),
):
    """Get one agent specified by UUID."""
    # First check is user has probing enabled
    if not user.probing_enabled:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You must have probing enabled to access this resource",
        )

    if agent := await redis.get_agent_by_uuid(uuid):
        return agent
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent not found")
