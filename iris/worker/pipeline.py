"""Measurement pipeline."""
from logging import Logger
from typing import Dict
from uuid import UUID

import aiofiles
from aiofiles import os as aios
from diamond_miner import mappers
from diamond_miner.defaults import (
    DEFAULT_PREFIX_LEN_V4,
    DEFAULT_PREFIX_LEN_V6,
    DEFAULT_PREFIX_SIZE_V4,
    DEFAULT_PREFIX_SIZE_V6,
)
from diamond_miner.queries import GetSlidingPrefixes
from diamond_miner.rounds.mda_parallel import mda_probes_parallel

from iris.commons.database import Agents, InsertResults
from iris.commons.redis import Redis
from iris.commons.round import Round
from iris.commons.schemas.private import MeasurementRequest
from iris.commons.storage import Storage
from iris.worker import WorkerSettings


async def default_pipeline(
    settings: WorkerSettings,
    measurement_request: MeasurementRequest,
    agent_uuid: UUID,
    results_filename: str,
    statistics: Dict,
    logger: Logger,
    redis: Redis,
    storage: Storage,
):
    """Process results and eventually request a new round."""
    agent = measurement_request.agent(agent_uuid)
    assert agent.uuid
    logger_prefix = f"{measurement_request.uuid} :: {agent.uuid} ::"
    logger.info(f"{logger_prefix} New measurement file detected")

    logger.info(f"{logger_prefix} Get agent information")
    agent_parameters = await redis.get_agent_parameters(agent.uuid)

    round_ = Round.decode_from_filename(results_filename)
    measurement_results_path = settings.WORKER_RESULTS_DIR_PATH / str(
        measurement_request.uuid
    )

    logger.info(f"{logger_prefix} {round_}")
    logger.info(f"{logger_prefix} Download results file")
    results_filepath = measurement_results_path / results_filename
    await storage.download_file(
        str(measurement_request.uuid), results_filename, results_filepath
    )

    logger.info(f"{logger_prefix} Delete results file from AWS S3")
    is_deleted = await storage.delete_file_no_check(
        str(measurement_request.uuid), results_filename
    )
    if not is_deleted:
        logger.error(
            f"{logger_prefix} Impossible to remove results file `{results_filename}`"
        )

    logger.info(f"{logger_prefix} Store probing statistics")
    database_agents = Agents(settings, logger)
    await database_agents.store_probing_statistics(
        measurement_request.uuid, agent.uuid, round_.encode(), statistics
    )

    database = InsertResults(settings, logger, measurement_request.uuid, agent.uuid)

    logger.info(f"{logger_prefix} Create results tables")
    await database.create_table()

    logger.info(f"{logger_prefix} Insert CSV file into results table")
    await database.insert_csv(results_filepath)

    logger.info(f"{logger_prefix} Insert prefixes")
    await database.insert_prefixes()

    logger.info(f"{logger_prefix} Insert links")
    await database.insert_links()

    if not settings.WORKER_DEBUG_MODE:
        logger.info(f"{logger_prefix} Remove local results file")
        await aios.remove(results_filepath)

    next_round = round_.next_round(agent.tool_parameters.global_max_ttl)

    next_round_csv_filename = (
        f"{agent.uuid}_next_round_csv_{next_round.encode()}.csv.zst"
    )
    next_round_csv_filepath = measurement_results_path / next_round_csv_filename

    if next_round.number == 1:
        # We are in a sub-round 1
        # Compute the list of the prefixes need to be probed in the next ttl window

        if round_.max_ttl < max(
            agent_parameters.min_ttl, agent.tool_parameters.global_min_ttl
        ):
            # In this case the window was below the agent's min TTL
            # and the measuremnt's min TTL.
            # So there is no response for this round
            # and we don't want to compute and send a prefix list to probe
            return next_round, None

        prefixes_to_probe = []
        # TODO: Fault-tolerency
        async for _, _, addr_v6 in GetSlidingPrefixes(
            window_min_ttl=round_.min_ttl,
            window_max_ttl=round_.max_ttl,
            stopping_condition=settings.WORKER_ROUND_1_STOPPING,
        ).execute_iter_async(
            settings.database_url(), f"{measurement_request.uuid}__{agent.uuid}"
        ):
            # TODO: Should we store the prefix length information in the database?
            if addr_v4 := addr_v6.ipv4_mapped:
                prefix = f"{addr_v4}/{DEFAULT_PREFIX_LEN_V4}"
            else:
                prefix = f"{addr_v6}/{DEFAULT_PREFIX_LEN_V6}"
            prefixes_to_probe.append(prefix)

        if prefixes_to_probe:
            # Write the prefix to be probed in a next round file
            async with aiofiles.open(next_round_csv_filepath, "w") as fd:
                await fd.writelines(prefix + "\n" for prefix in prefixes_to_probe)

            logger.info(f"{logger_prefix} Uploading next round CSV prefix file")
            await storage.upload_file(
                str(measurement_request.uuid),
                next_round_csv_filename,
                next_round_csv_filepath,
            )

            if not settings.WORKER_DEBUG_MODE:
                logger.info(f"{logger_prefix} Remove local next round CSV prefix file")
                await aios.remove(next_round_csv_filepath)
            return next_round, next_round_csv_filename
        else:
            # If there is no prefixes to probe left, skip the last sub-rounds
            # and directly go to round 2
            next_round = Round(2, 0, 0)
            next_round_csv_filename = (
                f"{agent.uuid}_next_round_csv_{next_round.encode()}.csv.zst"
            )
            next_round_csv_filepath = measurement_results_path / next_round_csv_filename

    if next_round.number > agent.tool_parameters.max_round:
        logger.info(f"{logger_prefix} Maximum round reached. Stopping.")
        return None, None

    logger.info(f"{logger_prefix} Compute the next round CSV probe file")
    flow_mapper_cls = getattr(mappers, agent.tool_parameters.flow_mapper)
    flow_mapper_kwargs = agent.tool_parameters.flow_mapper_kwargs or {}
    flow_mapper_v4 = flow_mapper_cls(
        **{"prefix_size": DEFAULT_PREFIX_SIZE_V4, **flow_mapper_kwargs}
    )
    flow_mapper_v6 = flow_mapper_cls(
        **{"prefix_size": DEFAULT_PREFIX_SIZE_V6, **flow_mapper_kwargs}
    )
    n_probes_to_send = await mda_probes_parallel(
        filepath=next_round_csv_filepath,
        url=settings.database_url(),
        measurement_id=f"{measurement_request.uuid}__{agent.uuid}",
        round_=round_.number,
        mapper_v4=flow_mapper_v4,
        mapper_v6=flow_mapper_v6,
        probe_src_port=agent.tool_parameters.initial_source_port,
        probe_dst_port=agent.tool_parameters.destination_port,
        adaptive_eps=True,
    )

    if n_probes_to_send > 0:
        logger.info(f"{logger_prefix} Next round is required")
        logger.info(f"{logger_prefix} Probes to send: {n_probes_to_send}")
        logger.info(f"{logger_prefix} Uploading next round CSV probe file")
        await storage.upload_file(
            str(measurement_request.uuid),
            next_round_csv_filename,
            next_round_csv_filepath,
        )

        if not settings.WORKER_DEBUG_MODE:
            logger.info(f"{logger_prefix} Remove local next round CSV probe file")
            await aios.remove(next_round_csv_filepath)
        return next_round, next_round_csv_filename

    else:
        logger.info(f"{logger_prefix} Next round is not required")
        if not settings.WORKER_DEBUG_MODE:
            logger.info(f"{logger_prefix} Remove local empty next round CSV probe file")
            try:
                await aios.remove(next_round_csv_filepath)
            except FileNotFoundError:
                pass
        return None, None
