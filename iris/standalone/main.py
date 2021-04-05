import asyncio
import logging
import sys
from functools import wraps
from typing import Optional

import typer

from iris.api.schemas import ToolParameters
from iris.standalone import Tool, default_parameters
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
    protocol: Optional[str] = typer.Option(default_parameters.protocol),
    initial_source_port: Optional[int] = typer.Option(
        default_parameters.initial_source_port
    ),
    destination_port: Optional[int] = typer.Option(default_parameters.destination_port),
    min_ttl: Optional[int] = typer.Option(default_parameters.min_ttl),
    max_ttl: Optional[int] = typer.Option(default_parameters.max_ttl),
    max_round: Optional[int] = typer.Option(default_parameters.max_round),
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
            "protocol": protocol,
            "initial_source_port": initial_source_port,
            "destination_port": destination_port,
            "min_ttl": min_ttl,
            "max_ttl": max_ttl,
            "max_round": max_round,
        }
    )

    # Create logger
    logger = create_logger(logging.DEBUG if verbose else logging.ERROR)

    # Launch pipeline
    pipeline_info = await pipeline(
        tool, prefixes, probing_rate, tool_parameters, logger
    )
    display_results(pipeline_info)


@app.command()
@coroutine
async def ping(
    probing_rate: int = typer.Argument(1000),
    protocol: Optional[str] = typer.Option(default_parameters.protocol),
    initial_source_port: Optional[int] = typer.Option(
        default_parameters.initial_source_port
    ),
    destination_port: Optional[int] = typer.Option(default_parameters.destination_port),
    min_ttl: Optional[int] = typer.Option(default_parameters.min_ttl),
    max_ttl: Optional[int] = typer.Option(default_parameters.max_ttl),
    max_round: Optional[int] = typer.Option(default_parameters.max_round),
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
            "protocol": protocol,
            "initial_source_port": initial_source_port,
            "destination_port": destination_port,
            "min_ttl": min_ttl,
            "max_ttl": max_ttl,
            "max_round": max_round,
        }
    )

    # Create logger
    logger = create_logger(logging.DEBUG if verbose else logging.ERROR)

    # Launch pipeline
    pipeline_info = await pipeline(
        tool, prefixes, probing_rate, tool_parameters, logger
    )
    display_results(pipeline_info)


if __name__ == "__main__":
    app()
