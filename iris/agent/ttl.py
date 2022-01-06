import csv
import io
import subprocess
from typing import List, Optional

from iris.commons.logger import base_logger


def build_cmd(d):
    for k, v in d.items():
        if not isinstance(v, bool) or v:
            yield "--" + k.replace("_", "-")
        if not isinstance(v, bool):
            yield str(v)


def mtr(destination, logger=base_logger, **kwargs):
    cmd = ["mtr", *build_cmd(kwargs), destination]
    try:
        logger.info(" ".join(cmd))
        result = subprocess.run(cmd, capture_output=True, check=True)
        return result.stdout.decode("utf-8")
    except subprocess.CalledProcessError as e:
        logger.error(e.stdout.decode("utf-8"))
        logger.error(e.stderr.decode("utf-8"))
        raise


def find_exit_ttl_from_output(
    output: str,
    min_ttl: int,
    *,
    excluded: Optional[List[str]] = None,
    logger=base_logger,
) -> Optional[int]:
    # Ensure that the exit TTL is never in one of these networks.
    # This can be useful if a spurious/invalid ASN appears
    # before the true "gateway" ASN.
    excluded = [None, "AS???"] + (excluded or [])  # type: ignore

    reader = csv.DictReader(io.StringIO(str(output)))
    hops = {int(row["Hop"]): row for row in reader}

    if not hops:
        logger.info("No response from MTR")
        return None

    # (current asn, first TTL where it appeared)
    curr_asn = (None, 0)
    max_ttl = max(hops.keys())

    for ttl in range(1, max_ttl + 1):
        logger.info("%s: %s", ttl, hops.get(ttl, {}).get("Asn", "*"))

    for ttl in range(min_ttl, max_ttl + 1):
        asn = hops.get(ttl, {}).get("Asn")
        if asn not in excluded:  # type: ignore
            if not curr_asn[0]:
                curr_asn = (asn, ttl)  # type: ignore
            elif curr_asn[0] != asn:
                curr_asn = (asn, ttl)
                break

    return max(min_ttl, curr_asn[1])


def find_exit_ttl_with_mtr(
    destination: str,
    min_ttl: int,
    *,
    excluded: Optional[List[str]] = None,
    logger=base_logger,
) -> Optional[int]:
    """Find the first TTL which is not in the source AS."""
    logger.info("Finding exit TTL towards %s...", destination)
    output = mtr(
        destination,
        logger=logger,
        aslookup=True,
        csv=True,
        gracetime=1,
        no_dns=True,
        report_cycles=1,
    )
    return find_exit_ttl_from_output(
        output=output, min_ttl=min_ttl, excluded=excluded, logger=logger
    )
