"""agents operations."""

from diamond_miner.api.models import (
    ExceptionResponse,
    AgentsGetResponse,
    AgentsGetByUUIDResponse,
)
from fastapi import APIRouter, Request, HTTPException

router = APIRouter()


@router.get("/", response_model=AgentsGetResponse)
async def get_agents(request: Request):
    """Get all agents information."""
    agents = await request.app.redis.get_agents(parameters=False)
    return {"count": len(agents), "results": agents}


@router.get(
    "/{uuid}",
    response_model=AgentsGetByUUIDResponse,
    responses={404: {"model": ExceptionResponse}, 500: {"model": ExceptionResponse}},
)
async def get_agent_by_uuid(request: Request, uuid: str):
    """Get agent information from agent UUID."""
    agents = await request.app.redis.get_agents(state=False, parameters=False)
    filtered_agents = [agent["uuid"] for agent in agents if agent["uuid"] == uuid]
    if len(filtered_agents) == 0:
        raise HTTPException(status_code=404, detail="Agent not found")
    elif len(filtered_agents) > 1:
        raise HTTPException(status_code=500, detail="Duplicate Redis UUID")

    agent_uuid = filtered_agents[0]
    agent_state = await request.app.redis.get_agent_state(agent_uuid)
    agent_parameters = await request.app.redis.get_agent_parameters(agent_uuid)
    return {"uuid": agent_uuid, "state": agent_state, "parameters": agent_parameters}


@router.post("/")
def post_agents():
    """Deploy agent into Kubernetes cluster."""
    return {}


@router.delete("/")
def delete_agents():
    """Undeploy agent into Kubernetes cluster."""
    return {}
