from collections import Counter

from aioboto3 import Session
from fastapi import APIRouter, Depends

from iris import __version__
from iris.commons.dependencies import get_redis, get_session, get_storage
from iris.commons.models import Measurement
from iris.commons.models.status import Status
from iris.commons.redis import Redis
from iris.commons.storage import Storage

router = APIRouter()


@router.get("/", response_model=Status, summary="Get Iris status")
async def get_status(
    redis: Redis = Depends(get_redis),
    session: Session = Depends(get_session),
    storage: Storage = Depends(get_storage),
):
    agents = await redis.get_agents()
    agents_by_state = Counter(a.state for a in agents)
    buckets = await storage.get_measurement_buckets()
    measurements = Measurement.all(session)
    measurements_by_state = Counter(m.state for m in measurements)
    return Status(
        agents=agents_by_state,
        buckets=len(buckets),
        measurements=measurements_by_state,
        version=__version__,
    )
