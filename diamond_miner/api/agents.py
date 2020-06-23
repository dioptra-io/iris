"""agents operations."""

import json

from fastapi import APIRouter, Request, HTTPException

router = APIRouter()


def state_formater(state):
    if state is None:
        return "unknown"
    elif state:
        return "idle"
    else:
        return "working"


@router.get("/")
async def get_agents(request: Request):
    """Get all agents information."""
    agents = await request.app.redis.agents_info()
    response = [
        {"uuid": agent, "state": state_formater(state)} for agent, state in agents
    ]
    return {"count": len(response), "results": response}


@router.get("/{uuid}")
async def get_agent_by_uuid(request: Request, uuid: str):
    """Get agent information from agent UUID."""
    agents = await request.app.redis.agents_info()
    filtered_agents = [(agent, state) for agent, state in agents if agent == uuid]
    if len(filtered_agents) == 0:
        raise HTTPException(status_code=404, detail="Agent not found")
    elif len(filtered_agents) > 1:
        raise HTTPException(status_code=500, detail="Duplicate Redis UUID")

    agent = filtered_agents[0]
    agent_uuid = agent[0]
    agent_status = agent[1]
    agent_parameters = await request.app.redis.get(f"parameters:{agent_uuid}")
    ip_address, probing_rate, buffer_sniffer_size = None, None, None
    if agent_parameters is not None:
        agent_parameters = json.loads(agent_parameters)
        ip_address, probing_rate, buffer_sniffer_size = (
            agent_parameters["ip_address"],
            agent_parameters["probing_rate"],
            agent_parameters["buffer_sniffer_size"],
        )
    return {
        "uuid": agent_uuid,
        "state": state_formater(agent_status),
        "ip_address": ip_address,
        "probing_rate": probing_rate,
        "buffer_sniffer_size": buffer_sniffer_size,
    }


@router.post("/")
def post_agents():
    """Deploy agent into Kubernetes cluster."""
    return {}


@router.delete("/")
def delete_agents():
    """Undeploy agent into Kubernetes cluster."""
    return {}
