"""Measurement pipeline."""

import asyncio
from concurrent.futures import ProcessPoolExecutor

from aiofiles import os as aios
from clickhouse_driver import Client
from diamond_miner import mappers
from diamond_miner.defaults import DEFAULT_PREFIX_SIZE_V4, DEFAULT_PREFIX_SIZE_V6
from diamond_miner.format import format_probe
from diamond_miner.rounds.mda import mda_probes
from diamond_miner.subsets import subsets_for_table

from iris.commons.database import DatabaseMeasurementResults, get_session
from iris.worker.shuffle import shuffle_next_round_csv


def extract_round_number(filename):
    return int(filename.split("_")[-1].split(".")[0])


def sync_compute_next_round(
    settings,
    table_name,
    round_number,
    parameters,
    next_round_csv_filepath,
):
    """Compute the next round synchronously."""
    flow_mapper_cls = getattr(mappers, parameters.tool_parameters["flow_mapper"])
    flow_mapper_kwargs = parameters.tool_parameters["flow_mapper_kwargs"] or {}
    flow_mapper_v4 = flow_mapper_cls(
        **{"prefix_size": DEFAULT_PREFIX_SIZE_V4, **flow_mapper_kwargs}
    )
    flow_mapper_v6 = flow_mapper_cls(
        **{"prefix_size": DEFAULT_PREFIX_SIZE_V6, **flow_mapper_kwargs}
    )
    client = Client(host=settings.DATABASE_HOST)
    probes_gen = mda_probes(
        client=client,
        table=table_name,
        round_=round_number,
        mapper_v4=flow_mapper_v4,
        mapper_v6=flow_mapper_v6,
        probe_src_addr=parameters.ip_address,
        probe_src_port=parameters.tool_parameters["initial_source_port"],
        probe_dst_port=parameters.tool_parameters["destination_port"],
        adaptive_eps=True,
        skip_unpopulated_ttls=True,
        subsets=subsets_for_table(client, table_name),
    )

    with open(next_round_csv_filepath, "w") as fout:
        for probes_specs in probes_gen:
            fout.write(
                "".join(("\n".join(format_probe(*spec) for spec in probes_specs), "\n"))
            )


async def default_pipeline(settings, parameters, results_filename, storage, logger):
    """Process results and eventually request a new round."""
    measurement_uuid = parameters.measurement_uuid
    agent_uuid = parameters.agent_uuid

    logger_prefix = f"{measurement_uuid} :: {agent_uuid} ::"
    logger.info(f"{logger_prefix} New files detected")

    round_number = extract_round_number(results_filename)
    measurement_results_path = settings.WORKER_RESULTS_DIR_PATH / measurement_uuid

    logger.info(f"{logger_prefix} Round {round_number}")
    logger.info(f"{logger_prefix} Download results file")
    results_filepath = str(measurement_results_path / results_filename)
    await storage.download_file(measurement_uuid, results_filename, results_filepath)

    logger.info(f"{logger_prefix} Delete results file from AWS S3")
    is_deleted = await storage.delete_file_no_check(measurement_uuid, results_filename)
    if not is_deleted:
        logger.error(f"Impossible to remove result file `{results_filename}`")

    session = get_session(settings)
    table_name = (
        settings.DATABASE_NAME
        + "."
        + DatabaseMeasurementResults.forge_table_name(measurement_uuid, agent_uuid)
    )
    database = DatabaseMeasurementResults(session, settings, table_name, logger=logger)

    logger.info(f"{logger_prefix} Create table `{table_name}`")
    await database.create_table()

    # NOTE: Temporarily deactivate the materialized vue for performance
    # logger.info(f"{logger_prefix} Create materialized vues for `{table_name}`")
    # await database.create_materialized_vue_nodes()
    # await database.create_materialized_vue_traceroute()

    logger.info(f"{logger_prefix} Insert CSV file into database")
    await database.insert_csv(results_filepath)

    if not settings.WORKER_DEBUG_MODE:
        logger.info(f"{logger_prefix} Remove local results file")
        await aios.remove(results_filepath)

    next_round_number = round_number + 1
    if next_round_number > parameters.tool_parameters["max_round"]:
        logger.info(f"{logger_prefix} Maximum round reached. Stopping.")
        return None

    logger.info(f"{logger_prefix} Compute the next round CSV probe file")
    next_round_csv_filename = f"{agent_uuid}_next_round_csv_{next_round_number}.csv"
    next_round_csv_filepath = str(measurement_results_path / next_round_csv_filename)

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(
        ProcessPoolExecutor(),
        sync_compute_next_round,
        settings,
        table_name,
        round_number,
        parameters,
        next_round_csv_filepath,
    )

    shuffled_next_round_csv_filename = (
        f"{agent_uuid}_shuffled_next_round_csv_{next_round_number}.csv"
    )
    shuffled_next_round_csv_filepath = str(
        measurement_results_path / shuffled_next_round_csv_filename
    )

    if (await aios.stat(next_round_csv_filepath)).st_size != 0:
        logger.info(f"{logger_prefix} Next round is required")
        logger.info(f"{logger_prefix} Shuffle next round CSV probe file")
        await shuffle_next_round_csv(
            settings,
            next_round_csv_filepath,
            shuffled_next_round_csv_filepath,
            logger,
            logger_prefix=logger_prefix + " ",
        )

        if not settings.WORKER_DEBUG_MODE:
            logger.info(f"{logger_prefix} Remove local next round CSV probe file")
            await aios.remove(next_round_csv_filepath)

        logger.info(f"{logger_prefix} Uploading shuffled next round CSV probe file")
        await storage.upload_file(
            measurement_uuid,
            shuffled_next_round_csv_filename,
            shuffled_next_round_csv_filepath,
        )

        if not settings.WORKER_DEBUG_MODE:
            logger.info(
                f"{logger_prefix} Remove local shuffled next round CSV probe file"
            )
            await aios.remove(shuffled_next_round_csv_filepath)
        return shuffled_next_round_csv_filename

    else:
        logger.info(f"{logger_prefix} Next round is not required")
        if not settings.WORKER_DEBUG_MODE:
            logger.info(f"{logger_prefix} Remove local empty next round CSV probe file")
            await aios.remove(next_round_csv_filepath)
        return None
