"""agents operations."""

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from iris.api.pagination import ListPagination
from iris.api.security import get_current_active_user
from iris.commons.redis import Redis
from iris.commons.schemas import public

router = APIRouter()


@router.get(
    "/",
    response_model=public.Paginated[public.Agent],
    summary="Get all agents.",
)
async def get_agents(
    request: Request,
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=0, le=200),
    user: str = Depends(get_current_active_user),
):
    """Get all agents."""
    redis: Redis = request.app.redis
    agents = await redis.get_agents()
    results = [
        {
            "uuid": agent.uuid,
            "state": agent.state,
            "parameters": agent.parameters,
        }
        for agent in agents
    ]
    querier = ListPagination(results, request, offset, limit)
    return await querier.query()


@router.get(
    "/{uuid}",
    response_model=public.Agent,
    responses={404: {"model": public.GenericException}},
    summary="Get agent specified by UUID.",
)
async def get_agent_by_uuid(
    request: Request, uuid: UUID, user: str = Depends(get_current_active_user)
):
    """Get one agent specified by UUID."""
    redis: Redis = request.app.redis
    agent = await redis.get_agent_by_uuid(uuid)
    if not agent:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Agent not found"
        )
    return {"uuid": agent.uuid, "state": agent.state, "parameters": agent.parameters}
