from multiprocessing import Manager, Process
from pathlib import Path

from iris.agent.prober import probe, watch_cancellation
from iris.agent.settings import AgentSettings
from iris.commons.models import MeasurementRoundRequest
from iris.commons.redis import Redis


async def caracal_inner_pipeline(
    settings: AgentSettings,
    request: MeasurementRoundRequest,
    redis: Redis,
    probes_filepath: Path,
    results_filepath: Path,
):
    with Manager() as manager:
        prober_statistics = manager.dict()  # type: ignore
        prober_process = Process(
            target=probe,
            args=(
                settings,
                probes_filepath,
                results_filepath,
                request.round.number,
                request.probing_rate,
                prober_statistics,
            ),
        )
        prober_process.start()
        is_not_canceled = await watch_cancellation(
            redis,
            prober_process,
            request.measurement_uuid,
            settings.AGENT_UUID,
            settings.AGENT_STOPPER_REFRESH,
        )
        prober_statistics = dict(prober_statistics)

    return prober_statistics, is_not_canceled
