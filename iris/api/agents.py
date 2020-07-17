"""agents operations."""

from fastapi import APIRouter, Depends, Request, HTTPException
from iris.api.security import authenticate
from iris.api.schemas import (
    ExceptionResponse,
    AgentsGetResponse,
    AgentsGetByUUIDResponse,
)

router = APIRouter()


@router.get("/", response_model=AgentsGetResponse, summary="Get all agents inforamtion")
async def get_agents(request: Request, username: str = Depends(authenticate)):
    """Get all agents information."""
    agents = await request.app.redis.get_agents(parameters=False)
    return {"count": len(agents), "results": agents}


@router.get(
    "/{uuid}",
    response_model=AgentsGetByUUIDResponse,
    responses={404: {"model": ExceptionResponse}, 500: {"model": ExceptionResponse}},
    summary="Get agent information from UUID",
)
async def get_agent_by_uuid(
    request: Request, uuid: str, username: str = Depends(authenticate)
):
    """Get agent information from UUID."""
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


# @router.post("/", summary="Deploy agents into Kubernetes cluster")
# def post_agents(username: str = Depends(authenticate)):
#     """Deploy agents into Kubernetes cluster."""
#     raise HTTPException(501, detail="Not implemented")


# @router.delete("/", summary="Undeploy agents from Kubernetes cluster")
# def delete_agents(username: str = Depends(authenticate)):
#     """Undeploy agents from Kubernetes cluster."""
#     raise HTTPException(501, detail="Not implemented")
