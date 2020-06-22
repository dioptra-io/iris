"""agents operations."""

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
    else:
        agent = filtered_agents[0]
        return {"uuid": agent[0], "state": state_formater(agent[1])}


@router.post("/")
def post_agents():
    """Deploy agent into Kubernetes cluster."""
    return {}


@router.delete("/")
def delete_agents():
    """Undeploy agent into Kubernetes cluster."""
    return {}
