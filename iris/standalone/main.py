import asyncio
import logging
import sys
from functools import wraps
from typing import List, Optional

import typer

from iris.api.schemas import Tool, ToolParameters
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
    prefixes: list = sys.stdin.readlines()
    if not prefixes:
        typer.echo("Please provide a prefixes list in stdin")
        raise typer.Exit()

    tool: Tool = Tool("diamond-miner")
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
        tool, prefixes, probing_rate, tool_parameters, tag, logger
    )
    display_results(pipeline_info)


@app.command()
@coroutine
async def yarrp(
    probing_rate: int = typer.Argument(1000),
    initial_source_port: Optional[int] = typer.Option(
        default_parameters.initial_source_port
    ),
    destination_port: Optional[int] = typer.Option(default_parameters.destination_port),
    tag: Optional[List[str]] = typer.Option(["standalone"]),
    verbose: bool = typer.Option(False, "--verbose"),
):
    """YARRP command."""
    prefixes: list = sys.stdin.readlines()
    if not prefixes:
        typer.echo("Please provide a prefixes list in stdin")
        raise typer.Exit()

    tool: Tool = Tool("yarrp")
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
        tool, prefixes, probing_rate, tool_parameters, tag, logger
    )
    display_results(pipeline_info)


@app.command()
@coroutine
async def ping(
    probing_rate: int = typer.Argument(1000),
    initial_source_port: Optional[int] = typer.Option(
        default_parameters.initial_source_port
    ),
    destination_port: Optional[int] = typer.Option(default_parameters.destination_port),
    tag: Optional[List[str]] = typer.Option(["standalone"]),
    verbose: bool = typer.Option(False, "--verbose"),
):
    """Ping command."""
    prefixes: list = sys.stdin.readlines()
    if not prefixes:
        typer.echo("Please provide a prefixes list in stdin")
        raise typer.Exit()

    tool = Tool("ping")
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
        tool, prefixes, probing_rate, tool_parameters, tag, logger
    )
    display_results(pipeline_info)


if __name__ == "__main__":
    app()
