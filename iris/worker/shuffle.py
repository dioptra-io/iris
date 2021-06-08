"""External shuffling processor interface."""

from iris.commons.subprocess import start_stream_subprocess


async def shuffle_next_round_csv(
    settings, csv_filepath, shuffled_csv_filepath, logger, logger_prefix=""
):
    cmd = (
        "export MEMORY="
        + str(settings.WORKER_TERASHUF_MEMORY)
        + "; export TMPDIR="
        + str(settings.WORKER_TERASHUF_TMP_DIR_PATH)
        + "; "
        + str(settings.WORKER_TERASHUF_PATH)
        + " < "
        + str(csv_filepath)
        + " > "
        + str(shuffled_csv_filepath)
    )

    await start_stream_subprocess(
        cmd,
        stdout=logger.debug,
        stderr=logger.info,
        prefix=logger_prefix,
    )
