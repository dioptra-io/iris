import asyncio
import logging
import sys
from functools import wraps
from pathlib import Path
from typing import List, Optional

import typer

from iris.commons.schemas.public.measurements import Tool, ToolParameters
from iris.standalone import default_parameters
from iris.standalone.display import display_results
from iris.standalone.logger import create_logger
from iris.standalone.pipeline import pipeline

app = typer.Typer()


def coroutine(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))

    return wrapper


@app.command()
@coroutine
async def diamond_miner(
    user: str = typer.Option("standalone"),
    prefix_list: Optional[Path] = typer.Option(None),
    probing_rate: int = typer.Argument(1000),
    initial_source_port: Optional[int] = typer.Option(
        default_parameters.initial_source_port
    ),
    destination_port: Optional[int] = typer.Option(default_parameters.destination_port),
    max_round: Optional[int] = typer.Option(default_parameters.max_round),
    tag: List[str] = typer.Option(["standalone"]),
    verbose: bool = typer.Option(False, "--verbose"),
):
    """Diamond-miner command."""
    if prefix_list:
        prefixes = prefix_list.read_text().splitlines()
    else:
        prefixes = sys.stdin.readlines()

    tool: Tool = Tool.DiamondMiner
    tool_parameters = ToolParameters(
        **{
            "initial_source_port": initial_source_port,
            "destination_port": destination_port,
            "max_round": max_round,
        }
    )

    # Create logger
    logger = create_logger(logging.DEBUG if verbose else logging.ERROR)

    # Launch pipeline
    pipeline_info = await pipeline(
        tool, prefixes, user, probing_rate, tool_parameters, tag, logger
    )
    display_results(pipeline_info)


@app.command()
@coroutine
async def yarrp(
    user: str = typer.Option("standalone"),
    prefix_list: Optional[Path] = typer.Option(None),
    probing_rate: int = typer.Argument(1000),
    initial_source_port: Optional[int] = typer.Option(
        default_parameters.initial_source_port
    ),
    destination_port: Optional[int] = typer.Option(default_parameters.destination_port),
    tag: Optional[List[str]] = typer.Option(["standalone"]),
    verbose: bool = typer.Option(False, "--verbose"),
):
    """YARRP command."""
    if prefix_list:
        prefixes = prefix_list.read_text().splitlines()
    else:
        prefixes = sys.stdin.readlines()

    tool: Tool = Tool.Yarrp
    tool_parameters = ToolParameters(
        **{
            "initial_source_port": initial_source_port,
            "destination_port": destination_port,
            "max_round": 1,
        }
    )

    # Create logger
    logger = create_logger(logging.DEBUG if verbose else logging.ERROR)

    # Launch pipeline
    pipeline_info = await pipeline(
        tool, prefixes, user, probing_rate, tool_parameters, tag or [], logger
    )
    display_results(pipeline_info)


@app.command()
@coroutine
async def ping(
    user: str = typer.Option("standalone"),
    prefix_list: Optional[Path] = typer.Option(None),
    probing_rate: int = typer.Argument(1000),
    initial_source_port: Optional[int] = typer.Option(
        default_parameters.initial_source_port
    ),
    destination_port: Optional[int] = typer.Option(default_parameters.destination_port),
    tag: Optional[List[str]] = typer.Option(["standalone"]),
    verbose: bool = typer.Option(False, "--verbose"),
):
    """Ping command."""
    if prefix_list:
        prefixes = prefix_list.read_text().splitlines()
    else:
        prefixes = sys.stdin.readlines()

    tool = Tool.Ping
    tool_parameters = ToolParameters(
        **{
            "initial_source_port": initial_source_port,
            "destination_port": destination_port,
            "max_round": 1,
        }
    )

    # Create logger
    logger = create_logger(logging.DEBUG if verbose else logging.ERROR)

    # Launch pipeline
    pipeline_info = await pipeline(
        tool, prefixes, user, probing_rate, tool_parameters, tag or [], logger
    )
    display_results(pipeline_info)


if __name__ == "__main__":
    app()
