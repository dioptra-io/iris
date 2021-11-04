"""Measurement pipeline."""
from dataclasses import dataclass
from logging import Logger
from typing import Optional
from uuid import UUID

from aiofiles import os as aios
from diamond_miner import mappers
from diamond_miner.generators import probe_generator_parallel
from diamond_miner.insert import insert_mda_probe_counts_parallel, insert_probe_counts
from diamond_miner.queries import GetSlidingPrefixes

from iris.commons.database import Database, InsertResults, agents
from iris.commons.redis import Redis
from iris.commons.schemas.private import MeasurementRequest
from iris.commons.schemas.public import ProbingStatistics, Round, Tool
from iris.commons.storage import Storage, next_round_key
from iris.worker.settings import WorkerSettings
from iris.worker.tree import load_targets


@dataclass(frozen=True)
class PipelineResult:
    round_: Optional[Round]
    prefix_filename: Optional[str]
    probe_filename: Optional[str]


async def default_pipeline(
    settings: WorkerSettings,
    measurement_request: MeasurementRequest,
    agent_uuid: UUID,
    results_filename: str,
    statistics: ProbingStatistics,
    logger: Logger,
    redis: Redis,
    storage: Storage,
) -> PipelineResult:
    """Process results and eventually request a new round."""
    agent = measurement_request.agent(agent_uuid)
    assert agent.uuid

    logger_prefix = f"{measurement_request.uuid} :: {agent.uuid} ::"
    logger.info(f"{logger_prefix} New measurement file detected")

    current_round = Round.decode(results_filename)
    next_round = current_round.next_round(agent.tool_parameters.global_max_ttl)
    logger.info(f"{logger_prefix} {current_round} => {next_round}")

    database = Database(settings, logger)
    database_results = InsertResults(
        database,
        measurement_request.uuid,
        agent.uuid,
        agent.tool_parameters.prefix_len_v4,
        agent.tool_parameters.prefix_len_v6,
    )

    database_url = settings.database_url()
    measurement_id = f"{measurement_request.uuid}__{agent.uuid}"
    measurement_results_path = settings.WORKER_RESULTS_DIR_PATH / str(
        measurement_request.uuid
    )

    logger.info(f"{logger_prefix} Get agent information")
    agent_parameters = await redis.get_agent_parameters(agent.uuid)

    logger.info(f"{logger_prefix} Download results file")
    results_filepath = await storage.download_file_to(
        storage.measurement_bucket(measurement_request.uuid),
        results_filename,
        measurement_results_path,
    )

    logger.info(f"{logger_prefix} Delete results file from S3")
    await storage.soft_delete(
        storage.measurement_bucket(measurement_request.uuid), results_filename
    )

    logger.info(f"{logger_prefix} Store probing statistics")
    await agents.store_probing_statistics(
        database, measurement_request.uuid, agent.uuid, statistics
    )

    logger.info(f"{logger_prefix} Create results tables")
    await database_results.create_table()

    logger.info(f"{logger_prefix} Insert CSV file into results table")
    await database_results.insert_csv(results_filepath)

    logger.info(f"{logger_prefix} Insert prefixes")
    await database_results.insert_prefixes()

    logger.info(f"{logger_prefix} Insert links")
    await database_results.insert_links()

    if not settings.WORKER_DEBUG_MODE:
        logger.info(f"{logger_prefix} Remove local results file")
        await aios.remove(results_filepath)

    if next_round.number == 1:
        # We are in a sub-round of round 1:
        # compute the list of the prefixes to probe in the next sub-round.
        if current_round.max_ttl < max(
            agent_parameters.min_ttl, agent.tool_parameters.global_min_ttl
        ):
            # The window was below the agent and the measurement minimum TTL.
            # As such no probes were sent and no replies were received.
            # We directly go to the next sub-round.
            return PipelineResult(
                round_=next_round, prefix_filename=None, probe_filename=None
            )

        # TODO: Handle the case for ping (where we take the max TTL)?
        # elif tool == Tool.Ping:
        #     # Only take the max TTL in the TTL range
        #     for line in target_list:
        #         prefix, protocol, min_ttl, max_ttl = line.split(",")
        #         prefixes.append((prefix, protocol, (int(max_ttl),)))
        prefixes = []

        with open(agent.target_file) as f:
            targets = load_targets(
                f,
                clamp_ttl_min=max(agent_parameters.min_ttl, next_round.min_ttl),
                clamp_ttl_max=next_round.max_ttl,
            )

        async for _, _, addr_v6 in GetSlidingPrefixes(
            window_max_ttl=current_round.max_ttl,
            stopping_condition=settings.WORKER_ROUND_1_STOPPING,
        ).execute_iter_async(database_url, measurement_id):
            if addr_v4 := addr_v6.ipv4_mapped:
                prefix = f"{addr_v4}/{agent.tool_parameters.prefix_len_v4}"
            else:
                prefix = f"{addr_v6}/{agent.tool_parameters.prefix_len_v6}"
            for protocol, ttls in targets[prefix]:
                prefixes.append((prefix, protocol, ttls))

        if prefixes:
            insert_probe_counts(
                url=database_url,
                measurement_id=measurement_id,
                round_=1,
                prefixes=prefixes,
                prefix_len_v4=agent.tool_parameters.prefix_len_v4,
                prefix_len_v6=agent.tool_parameters.prefix_len_v6,
            )
        else:
            # If there are no remaining prefixes to probe,
            # skip the last sub-rounds and directly go to round 2.
            next_round = Round(number=2, limit=0, offset=0)

    if next_round.number > agent.tool_parameters.max_round:
        logger.info(f"{logger_prefix} Maximum round reached. Stopping.")
        return PipelineResult(round_=None, prefix_filename=None, probe_filename=None)

    if next_round.number > 1 and measurement_request.tool != Tool.DiamondMiner:
        logger.info(f"{logger_prefix} Tool does not support rounds > 1. Stopping.")
        return PipelineResult(round_=None, prefix_filename=None, probe_filename=None)

    if next_round.number > 1:
        await insert_mda_probe_counts_parallel(
            url=database_url,
            measurement_id=measurement_id,
            previous_round=next_round.number - 1,
            adaptive_eps=True,
            target_epsilon=agent.tool_parameters.failure_rate,
        )

    logger.info(f"{logger_prefix} Compute the next round CSV probe file")
    next_round_csv_filepath = measurement_results_path / next_round_key(
        agent.uuid, next_round
    )
    flow_mapper_cls = getattr(mappers, agent.tool_parameters.flow_mapper)
    flow_mapper_kwargs = agent.tool_parameters.flow_mapper_kwargs or {}
    flow_mapper_v4 = flow_mapper_cls(
        **{"prefix_size": agent.tool_parameters.prefix_size_v4, **flow_mapper_kwargs}
    )
    flow_mapper_v6 = flow_mapper_cls(
        **{"prefix_size": agent.tool_parameters.prefix_size_v6, **flow_mapper_kwargs}
    )
    n_probes_to_send = await probe_generator_parallel(
        filepath=next_round_csv_filepath,
        url=database_url,
        measurement_id=measurement_id,
        round_=next_round.number,
        mapper_v4=flow_mapper_v4,
        mapper_v6=flow_mapper_v6,
        probe_src_port=agent.tool_parameters.initial_source_port,
        probe_dst_port=agent.tool_parameters.destination_port,
    )

    if n_probes_to_send > 0:
        logger.info(f"{logger_prefix} Next round is required")
        logger.info(f"{logger_prefix} Probes to send: {n_probes_to_send}")
        logger.info(f"{logger_prefix} Uploading next round CSV probe file")
        await storage.upload_file(
            storage.measurement_bucket(measurement_request.uuid),
            next_round_key(agent.uuid, current_round),
            next_round_csv_filepath,
        )

        if not settings.WORKER_DEBUG_MODE:
            logger.info(f"{logger_prefix} Remove local next round CSV probe file")
            await aios.remove(next_round_csv_filepath)
        return PipelineResult(
            round_=next_round,
            prefix_filename=None,
            probe_filename=next_round_key(agent.uuid, current_round),
        )

    else:
        logger.info(f"{logger_prefix} Next round is not required")
        if not settings.WORKER_DEBUG_MODE:
            logger.info(f"{logger_prefix} Remove local empty next round CSV probe file")
            try:
                await aios.remove(next_round_csv_filepath)
            except FileNotFoundError:
                pass
        return PipelineResult(round_=None, prefix_filename=None, probe_filename=None)
