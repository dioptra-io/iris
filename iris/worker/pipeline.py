"""Measurement pipeline."""

import aiofiles
from aioch import Client
from aiofiles import os as aios
from diamond_miner import mappers
from diamond_miner.config import Config
from diamond_miner.next_round import compute_next_round
from diamond_miner.utilities import format_probe

from iris.commons.database import DatabaseMeasurementResults, get_session
from iris.worker.shuffle import shuffle_next_round_csv


def extract_round_number(filename):
    return int(filename.split("_")[-1].split(".")[0])


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

    flow_mapper_cls = getattr(mappers, parameters.tool_parameters["flow_mapper"])
    flow_mapper_kwargs = parameters.tool_parameters["flow_mapper_kwargs"] or {}
    flow_mapper = flow_mapper_cls(**flow_mapper_kwargs)

    client = Client(host=settings.DATABASE_HOST)

    probes_gen = compute_next_round(
        client=client,
        table=table_name,
        round_=round_number,
        config=Config(
            adaptive_eps=True,
            far_ttl_min=20,
            far_ttl_max=40,
            mapper=flow_mapper,
            max_replies_per_subset=64_000_000,
            probe_src_addr=parameters.ip_address,
            probe_src_port=parameters.tool_parameters["initial_source_port"],
            probe_dst_port=parameters.tool_parameters["destination_port"],
            probe_far_ttls=False,
            skip_unpopulated_ttls=True,
        ),
    )

    async with aiofiles.open(next_round_csv_filepath, "w") as fout:
        async for probes_specs in probes_gen:
            await fout.write(
                "".join(("\n".join(format_probe(*spec) for spec in probes_specs), "\n"))
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
