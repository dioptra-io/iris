import asyncio
import aioboto3
import dramatiq

from diamond_miner.commons.settings import Settings
from diamond_miner.worker import logger


settings = Settings()

aws_settings = {
    "aws_access_key_id": settings.AWS_ACCESS_KEY_ID,
    "aws_secret_access_key": settings.AWS_SECRET_ACCESS_KEY,
    "endpoint_url": settings.AWS_S3_HOST,
    "region_name": settings.AWS_REGION_NAME,
}


async def pipeline(measurement_uuid, agent_uuid, result_filename):
    """Process result file and eventually request a new round."""
    logger.info("New result file detected! Processing ...")
    async with aioboto3.client("s3", **aws_settings) as s3:
        response = await s3.delete_object(Bucket=measurement_uuid, Key=result_filename)
        if response["ResponseMetadata"]["HTTPStatusCode"] != 204:
            logger.error(f"Impossible to remove result file `{result_filename}`")


async def watcher(measurement_uuid, agent):
    """Watch for a result file from an agent."""
    agent_uuid = agent[3]

    found = False
    while True:
        async with aioboto3.resource("s3", **aws_settings) as s3:
            bucket = await s3.Bucket(measurement_uuid)
            async for file_object in bucket.objects.all():
                if file_object.key.startswith(agent_uuid):
                    await pipeline(measurement_uuid, agent_uuid, file_object.key)
                    found = True
                    break
            if found:
                break
        await asyncio.sleep(1)


async def handle(measurement_uuid, agents):
    await asyncio.gather(*[watcher(measurement_uuid, agent) for agent in agents])
    async with aioboto3.client("s3", **aws_settings) as s3:
        await s3.delete_bucket(Bucket=measurement_uuid)
    return True


@dramatiq.actor(time_limit=settings.WATCHER_TIMEOUT)
def handler(measurement_uuid, agents):
    """Handle the output of a measurement."""
    asyncio.run(handle(measurement_uuid, agents))
