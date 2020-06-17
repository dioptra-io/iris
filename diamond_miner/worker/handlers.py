import asyncio
import dramatiq

from diamond_miner.commons.storage import Storage
from diamond_miner.worker.settings import WorkerSettings
from diamond_miner.worker import logger


settings = WorkerSettings()
storage = Storage()


async def pipeline(measurement_uuid, agent_uuid, result_filename):
    """Process result file and eventually request a new round."""
    logger.info("New result file detected! Processing ...")
    response = await storage.delete_file_no_check(measurement_uuid, result_filename)
    if response["ResponseMetadata"]["HTTPStatusCode"] != 204:
        logger.error(f"Impossible to remove result file `{result_filename}`")


async def watcher(measurement_uuid, agent):
    """Watch for a result file from an agent."""
    agent_uuid = agent[3]

    found = False
    while True:
        logger.info(f"{measurement_uuid}, {agent_uuid}")
        result_files = await storage.get_all_files(measurement_uuid)
        for result_file in result_files:
            if result_file["key"].startswith(agent_uuid):
                await pipeline(measurement_uuid, agent_uuid, result_file["key"])
                found = True
                break
        if found:
            break
        await asyncio.sleep(1)


async def handle(measurement_uuid, agents):
    await asyncio.gather(*[watcher(measurement_uuid, agent) for agent in agents])
    try:
        await storage.delete_bucket(bucket=measurement_uuid)
    except Exception:
        logger.error(f"Impossible to remove bucket `{measurement_uuid}`")
    return True


@dramatiq.actor(time_limit=settings.WATCHER_TIMEOUT)
def handler(measurement_uuid, agents):
    """Handle the output of a measurement."""
    asyncio.run(handle(measurement_uuid, agents))
