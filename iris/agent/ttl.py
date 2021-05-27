import csv
import io
import subprocess


def build_cmd(d):
    for k, v in d.items():
        if not isinstance(v, bool) or v:
            yield "--" + k.replace("_", "-")
        if not isinstance(v, bool):
            yield str(v)


def mtr(logger, destination, **kwargs):
    cmd = ["mtr", *build_cmd(kwargs), destination]
    try:
        logger.info("%s", " ".join(cmd))
        result = subprocess.run(cmd, capture_output=True, check=True)
        return result.stdout.decode("utf-8")
    except subprocess.CalledProcessError as e:
        print(e.stdout.decode("utf-8"))
        print(e.stderr.decode("utf-8"))
        raise


def find_exit_ttl(logger, destination, min_ttl):
    """Find the first TTL which is not in the source AS."""
    logger.info(f"Finding exit TTL towards {destination}...")

    # Ensure that the exit TTL is never in one of these networks.
    # This can be useful if a spurious/invalid ASN appears before the true "gateway" ASN.
    excluded = [
        None,
        "AS???",
        "AS2200",  # Renater
    ]

    out = mtr(
        logger,
        destination,
        aslookup=True,
        csv=True,
        gracetime=1,
        no_dns=True,
        report_cycles=1,
    )
    reader = csv.DictReader(io.StringIO(out))
    hops = {int(row["Hop"]): row for row in reader}

    # (current asn, first TTL where it appeared)
    curr_asn = (None, 0)
    max_ttl = max(hops.keys())

    for ttl in range(1, max_ttl + 1):
        logger.info("%s: %s", ttl, hops.get(ttl, {}).get("Asn", "*"))

    for ttl in range(min_ttl, max_ttl + 1):
        asn = hops.get(ttl, {}).get("Asn")
        if asn not in excluded:
            if not curr_asn[0]:
                curr_asn = (asn, ttl)
            elif curr_asn[0] != asn:
                curr_asn = (asn, ttl)
                break

    exit_ttl = max(min_ttl, curr_asn[1])
    logger.info("Exit TTL: %s", exit_ttl)

    return exit_ttl
