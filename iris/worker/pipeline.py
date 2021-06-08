"""Measurement pipeline."""

from aiofiles import os as aios
from clickhouse_driver import Client
from diamond_miner import mappers
from diamond_miner.defaults import DEFAULT_PREFIX_SIZE_V4, DEFAULT_PREFIX_SIZE_V6
from diamond_miner.rounds.mda_parallel import mda_probes_parallel

from iris.commons.database import DatabaseMeasurementResults, get_session


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
    results_filepath = measurement_results_path / results_filename
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

    flows_view_name = database.swap_table_name_prefix("flows")
    links_table_name = database.swap_table_name_prefix("links")

    logger.info(f"{logger_prefix} Create results table `{table_name}`")
    await database.create_table()

    logger.info(f"{logger_prefix} Create view `{flows_view_name}`")
    await database.create_view_flows(flows_view_name)

    logger.info(f"{logger_prefix} Create links table `{links_table_name}`")
    await database.create_links_table(links_table_name)

    logger.info(f"{logger_prefix} Insert CSV file into results table")
    await database.insert_csv(results_filepath)

    logger.info(f"{logger_prefix} Insert links into links table")
    await database.insert_links(flows_view_name, links_table_name, round_number)

    if not settings.WORKER_DEBUG_MODE:
        logger.info(f"{logger_prefix} Remove local results file")
        await aios.remove(results_filepath)

    next_round_number = round_number + 1
    if next_round_number > parameters.tool_parameters["max_round"]:
        logger.info(f"{logger_prefix} Maximum round reached. Stopping.")
        return None

    logger.info(f"{logger_prefix} Compute the next round CSV probe file")
    next_round_csv_filename = f"{agent_uuid}_next_round_csv_{next_round_number}.csv.zst"
    next_round_csv_filepath = measurement_results_path / next_round_csv_filename

    flow_mapper_cls = getattr(mappers, parameters.tool_parameters["flow_mapper"])
    flow_mapper_kwargs = parameters.tool_parameters["flow_mapper_kwargs"] or {}
    flow_mapper_v4 = flow_mapper_cls(
        **{"prefix_size": DEFAULT_PREFIX_SIZE_V4, **flow_mapper_kwargs}
    )
    flow_mapper_v6 = flow_mapper_cls(
        **{"prefix_size": DEFAULT_PREFIX_SIZE_V6, **flow_mapper_kwargs}
    )
    client = Client(host=settings.DATABASE_HOST)
    n_probes_to_send = await mda_probes_parallel(
        filepath=next_round_csv_filepath,
        client=client,
        table=links_table_name,
        round_=round_number,
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
        return next_round_csv_filename

    else:
        logger.info(f"{logger_prefix} Next round is not required")
        if not settings.WORKER_DEBUG_MODE:
            logger.info(f"{logger_prefix} Remove local empty next round CSV probe file")
            await aios.remove(next_round_csv_filepath)
        return None
