from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from iris.api.authentication import current_active_user
from iris.commons.dependencies import get_redis
from iris.commons.models import Agent, Paginated, User
from iris.commons.redis import Redis

router = APIRouter()


@router.get("/", response_model=Paginated[Agent], summary="Get all agents.")
async def get_agents(
    request: Request,
    tag: str | None = None,
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=0, le=200),
    user: User = Depends(current_active_user),
    redis: Redis = Depends(get_redis),
):
    agents = await redis.get_agents()
    if tag:
        agents = [agent for agent in agents if tag in agent.parameters.tags]
    return Paginated.from_results(
        request.url, agents[offset : offset + limit], len(agents), offset, limit
    )


@router.get("/{uuid}", response_model=Agent, summary="Get agent specified by UUID.")
async def get_agent_by_uuid(
    uuid: UUID,
    user: User = Depends(current_active_user),
    redis: Redis = Depends(get_redis),
):
    if agent := await redis.get_agent_by_uuid(str(uuid)):
        return agent
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent not found")
