"""Measurement pipeline."""

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
from iris.commons.round import Round
from iris.worker import WorkerSettings


async def default_pipeline(
    settings: WorkerSettings, parameters, results_filename, statistics, storage, logger
):
    """Process results and eventually request a new round."""
    measurement_uuid = parameters.measurement_uuid
    agent_uuid = parameters.agent_uuid

    logger_prefix = f"{measurement_uuid} :: {agent_uuid} ::"
    logger.info(f"{logger_prefix} New measurement file detected")

    round = Round.decode_from_filename(results_filename)
    measurement_results_path = settings.WORKER_RESULTS_DIR_PATH / measurement_uuid

    logger.info(f"{logger_prefix} {round}")
    logger.info(f"{logger_prefix} Download results file")
    results_filepath = measurement_results_path / results_filename
    await storage.download_file(measurement_uuid, results_filename, results_filepath)

    logger.info(f"{logger_prefix} Delete results file from AWS S3")
    is_deleted = await storage.delete_file_no_check(measurement_uuid, results_filename)
    if not is_deleted:
        logger.error(
            f"{logger_prefix} Impossible to remove results file `{results_filename}`"
        )

    logger.info(f"{logger_prefix} Store probing statistics")
    database_agents = Agents(settings, logger)
    await database_agents.store_probing_statistics(
        measurement_uuid, agent_uuid, round.encode(), statistics
    )

    database = InsertResults(settings, logger, measurement_uuid, agent_uuid)

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

    next_round = round.next_round(parameters.tool_parameters["global_max_ttl"])

    next_round_csv_filename = (
        f"{agent_uuid}_next_round_csv_{next_round.encode()}.csv.zst"
    )
    next_round_csv_filepath = measurement_results_path / next_round_csv_filename

    if next_round.number == 1:
        # We are in a sub-round 1
        # Compute the list of the prefixes need to be probed in the next ttl window

        if round.max_ttl < max(
            parameters.min_ttl, parameters.tool_parameters["global_min_ttl"]
        ):
            # In this case the window was below the agent's min TTL
            # and the measuremnt's min TTL.
            # So there is no response for this round
            # and we don't want to compute and send a prefix list to probe
            return (next_round, None)

        prefixes_to_probe = []
        # TODO: Fault-tolerency
        async for _, _, addr_v6 in GetSlidingPrefixes(
            window_min_ttl=round.min_ttl,
            window_max_ttl=round.max_ttl,
            stopping_condition=settings.WORKER_ROUND_1_STOPPING,
        ).execute_iter_async(
            settings.database_url(), f"{measurement_uuid}__{agent_uuid}"
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
                measurement_uuid,
                next_round_csv_filename,
                next_round_csv_filepath,
            )

            if not settings.WORKER_DEBUG_MODE:
                logger.info(f"{logger_prefix} Remove local next round CSV prefix file")
                await aios.remove(next_round_csv_filepath)
            return (next_round, next_round_csv_filename)
        else:
            # If there is no prefixes to probe left, skip the last sub-rounds
            # and directly go to round 2
            next_round = Round(2, 0, 0)
            next_round_csv_filename = (
                f"{agent_uuid}_next_round_csv_{next_round.encode()}.csv.zst"
            )
            next_round_csv_filepath = measurement_results_path / next_round_csv_filename

    if next_round.number > parameters.tool_parameters["max_round"]:
        logger.info(f"{logger_prefix} Maximum round reached. Stopping.")
        return (None, None)

    logger.info(f"{logger_prefix} Compute the next round CSV probe file")
    flow_mapper_cls = getattr(mappers, parameters.tool_parameters["flow_mapper"])
    flow_mapper_kwargs = parameters.tool_parameters["flow_mapper_kwargs"] or {}
    flow_mapper_v4 = flow_mapper_cls(
        **{"prefix_size": DEFAULT_PREFIX_SIZE_V4, **flow_mapper_kwargs}
    )
    flow_mapper_v6 = flow_mapper_cls(
        **{"prefix_size": DEFAULT_PREFIX_SIZE_V6, **flow_mapper_kwargs}
    )
    n_probes_to_send = await mda_probes_parallel(
        filepath=next_round_csv_filepath,
        url=settings.database_url(),
        measurement_id=f"{measurement_uuid}__{agent_uuid}",
        round_=round.number,
        mapper_v4=flow_mapper_v4,
        mapper_v6=flow_mapper_v6,
        probe_src_port=parameters.tool_parameters["initial_source_port"],
        probe_dst_port=parameters.tool_parameters["destination_port"],
        adaptive_eps=True,
    )

    if n_probes_to_send > 0:
        logger.info(f"{logger_prefix} Next round is required")
        logger.info(f"{logger_prefix} Probes to send: {n_probes_to_send}")
        logger.info(f"{logger_prefix} Uploading next round CSV probe file")
        await storage.upload_file(
            measurement_uuid,
            next_round_csv_filename,
            next_round_csv_filepath,
        )

        if not settings.WORKER_DEBUG_MODE:
            logger.info(f"{logger_prefix} Remove local next round CSV probe file")
            await aios.remove(next_round_csv_filepath)
        return (next_round, next_round_csv_filename)

    else:
        logger.info(f"{logger_prefix} Next round is not required")
        if not settings.WORKER_DEBUG_MODE:
            logger.info(f"{logger_prefix} Remove local empty next round CSV probe file")
            try:
                await aios.remove(next_round_csv_filepath)
            except FileNotFoundError:
                pass
        return (None, None)
