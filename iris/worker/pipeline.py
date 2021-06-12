"""Measurement pipeline."""

import aiofiles
from aiofiles import os as aios
from diamond_miner import mappers
from diamond_miner.defaults import DEFAULT_PREFIX_SIZE_V4, DEFAULT_PREFIX_SIZE_V6
from diamond_miner.queries import GetSlidingPrefixes
from diamond_miner.queries.query import AddrType
from diamond_miner.rounds.mda_parallel import mda_probes_parallel

from iris.commons.database import DatabaseMeasurementResults, get_session, get_url
from iris.commons.round import Round


async def default_pipeline(settings, parameters, results_filename, storage, logger):
    """Process results and eventually request a new round."""
    measurement_uuid = parameters.measurement_uuid
    agent_uuid = parameters.agent_uuid

    logger_prefix = f"{measurement_uuid} :: {agent_uuid} ::"
    logger.info(f"{logger_prefix} New files detected")

    round = Round.decode_from_filename(results_filename)
    measurement_results_path = settings.WORKER_RESULTS_DIR_PATH / measurement_uuid

    logger.info(f"{logger_prefix} {round}")
    logger.info(f"{logger_prefix} Download results file")
    results_filepath = measurement_results_path / results_filename
    await storage.download_file(measurement_uuid, results_filename, results_filepath)

    logger.info(f"{logger_prefix} Delete results file from AWS S3")
    is_deleted = await storage.delete_file_no_check(measurement_uuid, results_filename)
    if not is_deleted:
        logger.error(f"Impossible to remove result file `{results_filename}`")

    session = get_session(settings)
    database = DatabaseMeasurementResults(
        session, settings, measurement_uuid, agent_uuid, logger=logger
    )

    logger.info(f"{logger_prefix} Create results tables")
    await database.create_table()

    logger.info(f"{logger_prefix} Insert CSV file into results table")
    await database.insert_csv(results_filepath)

    logger.info(f"{logger_prefix} Insert prefixes")
    await database.insert_prefixes(round.number)

    logger.info(f"{logger_prefix} Insert links")
    await database.insert_links(round.number)

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
        # TODO: Fault-tolerency
        prefixes_to_probe = []
        async for _, _, prefix in GetSlidingPrefixes(
            addr_type=AddrType.IPv6NumToString,
            window_min_ttl=round.min_ttl,
            window_max_ttl=round.max_ttl,
            stopping_condition=settings.WORKER_ROUND_1_STOPPING,
        ).execute_iter_async(get_url(settings), f"{measurement_uuid}__{agent_uuid}"):
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
        url=get_url(settings),
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
            await aios.remove(next_round_csv_filepath)
        return (None, None)
