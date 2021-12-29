"""Measurement pipeline."""
from dataclasses import dataclass
from logging import LoggerAdapter
from pathlib import Path
from typing import List, Optional

from sqlmodel import Session

from iris.commons.clickhouse import ClickHouse
from iris.commons.models.diamond_miner import Tool, ToolParameters
from iris.commons.models.round import Round
from iris.commons.redis import Redis
from iris.commons.storage import Storage, next_round_key
from iris.worker.inner_pipeline import inner_pipeline_for_tool


@dataclass(frozen=True)
class OuterPipelineResult:
    next_round: Round
    probes_key: str


async def outer_pipeline(
    clickhouse: ClickHouse,
    storage: Storage,
    session: Session,
    redis: Redis,
    logger: LoggerAdapter,
    measurement_uuid: str,
    agent_uuid: str,
    measurement_tags: List[str],
    # NOTE: See comments about these parameters in inner_pipeline.py.
    sliding_window_size: int,
    sliding_window_stopping_condition: int,
    tool: Tool,
    tool_parameters: ToolParameters,
    working_directory: Path,
    targets_key: str,
    results_key: Optional[str],
    user_id: str,
    debug_mode: bool = False,
) -> Optional[OuterPipelineResult]:
    """
    Responsible to download/upload from object storage.
    """
    logger.info("Running outer pipeline with results key %s", results_key)

    logger.info("Retrieve agent information from redis")
    agent_parameters = await redis.get_agent_parameters(agent_uuid)

    logger.info("Retrieve probing statistics from redis")
    if probing_statistics := await redis.get_measurement_stats(
        measurement_uuid, agent_uuid
    ):
        logger.info("Store probing statistics into the database")
        # TODO
        # await agents.store_probing_statistics(
        #     database, measurement_uuid, agent_uuid, probing_statistics
        # )
        logger.info("Remove probing statistics from redis")
        await redis.delete_measurement_stats(measurement_uuid, agent_uuid)

    logger.info("Download target file from object storage")
    targets_filepath = await storage.download_file_to(
        storage.targets_bucket(user_id), targets_key, working_directory
    )

    if results_key:
        logger.info("Download results file from object storage")
        results_filepath = await storage.download_file_to(
            storage.measurement_bucket(measurement_uuid),
            results_key,
            working_directory,
        )
        logger.info("Delete results file from object storage")
        await storage.soft_delete(
            storage.measurement_bucket(measurement_uuid), results_key
        )
    else:
        results_filepath = None

    if results_key:
        previous_round = Round.decode(results_key)
        next_round = previous_round.next_round(tool_parameters.global_max_ttl)
    else:
        previous_round = None
        next_round = Round(number=1, limit=sliding_window_size, offset=0)
        # Shift the window until it is above the min TTL of the agent
        # and the min TTL of the measurement.
        while next_round.max_ttl < max(
            agent_parameters.min_ttl, tool_parameters.global_min_ttl
        ):
            next_round = next_round.next_round(tool_parameters.global_max_ttl)
    logger.info("%s => %s", previous_round, next_round)

    probes_filepath = working_directory / next_round_key(agent_uuid, next_round)
    inner_pipeline_kwargs = dict(
        clickhouse=clickhouse,
        logger=logger,
        measurement_uuid=measurement_uuid,
        agent_uuid=agent_uuid,
        measurement_tags=measurement_tags,
        agent_min_ttl=agent_parameters.min_ttl,
        sliding_window_stopping_condition=sliding_window_stopping_condition,
        tool=tool,
        tool_parameters=tool_parameters,
        results_filepath=results_filepath,
        targets_filepath=targets_filepath,
        probes_filepath=probes_filepath,
        previous_round=previous_round,
        next_round=next_round,
    )
    n_probes_to_send = await inner_pipeline_for_tool[tool](**inner_pipeline_kwargs)

    if next_round.number > tool_parameters.max_round:
        # NOTE: We stop if we reached the maximum number of rounds.
        # Here we could do refactor to stop before computing the next round.
        logger.info("Maximum number of rounds reached")
        return None

    if next_round.number == 1 and n_probes_to_send == 0:
        logger.info("No remaining prefixes to probe at round 1. Going to round 2.")
        next_round = Round(number=2, limit=0, offset=0)
        probes_filepath = working_directory / next_round_key(agent_uuid, next_round)
        inner_pipeline_kwargs = {
            **inner_pipeline_kwargs,
            "probes_filepath": probes_filepath,
        }
        n_probes_to_send = await inner_pipeline_for_tool[tool](**inner_pipeline_kwargs)

    logger.info("Probes to send: %s", n_probes_to_send)
    result = None

    if n_probes_to_send > 0:
        logger.info("Upload probes file to object storage")
        await storage.upload_file(
            storage.measurement_bucket(measurement_uuid),
            probes_filepath.name,
            probes_filepath,
        )
        result = OuterPipelineResult(
            next_round=next_round, probes_key=probes_filepath.name
        )

    if not debug_mode:
        if targets_filepath:
            logger.info("Remove local targets file")
            targets_filepath.unlink(missing_ok=True)
        if results_filepath:
            logger.info("Remove local results file")
            results_filepath.unlink(missing_ok=True)
        if probes_filepath:
            logger.info("Remove local probes file")
            probes_filepath.unlink(missing_ok=True)

    return result
