"""agents operations."""

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from iris.api import schemas
from iris.api.pagination import ListPagination
from iris.api.security import get_current_active_user

router = APIRouter()


@router.get("/", response_model=schemas.Agents, summary="Get all agents.")
async def get_agents(
    request: Request,
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=0, le=200),
    user: str = Depends(get_current_active_user),
):
    """Get all agents."""
    agents_info = await request.app.redis.get_agents()
    agents = []

    for agent in agents_info:
        agents.append(
            {
                "uuid": agent["uuid"],
                "state": agent["state"],
                "parameters": agent["parameters"],
            }
        )

    querier = ListPagination(agents, request, offset, limit)
    return await querier.query()


@router.get(
    "/{uuid}",
    response_model=schemas.Agent,
    responses={
        404: {"model": schemas.GenericException},
        500: {"model": schemas.GenericException},
    },
    summary="Get agent specified by UUID.",
)
async def get_agent_by_uuid(
    request: Request, uuid: UUID, user: str = Depends(get_current_active_user)
):
    """Get one agent specified by UUID."""
    agents = await request.app.redis.get_agents(state=False, parameters=False)
    filtered_agents = [agent["uuid"] for agent in agents if agent["uuid"] == str(uuid)]
    if len(filtered_agents) == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Agent not found"
        )
    elif len(filtered_agents) > 1:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Duplicate Redis UUID",
        )

    agent_uuid = filtered_agents[0]
    agent_state = await request.app.redis.get_agent_state(agent_uuid)
    agent_parameters = await request.app.redis.get_agent_parameters(agent_uuid)
    return {"uuid": agent_uuid, "state": agent_state, "parameters": agent_parameters}
