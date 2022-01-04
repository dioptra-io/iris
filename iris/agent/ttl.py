import csv
import io
import subprocess
from typing import List, Optional


def build_cmd(d):
    for k, v in d.items():
        if not isinstance(v, bool) or v:
            yield "--" + k.replace("_", "-")
        if not isinstance(v, bool):
            yield str(v)


def mtr(destination, **kwargs):
    cmd = ["mtr", *build_cmd(kwargs), destination]
    try:
        print(" ".join(cmd))
        result = subprocess.run(cmd, capture_output=True, check=True)
        return result.stdout.decode("utf-8")
    except subprocess.CalledProcessError as e:
        print(e.stdout.decode("utf-8"))
        print(e.stderr.decode("utf-8"))
        raise


def find_exit_ttl_from_output(
    output: str, min_ttl: int, *, excluded: Optional[List[str]] = None
) -> Optional[int]:
    # Ensure that the exit TTL is never in one of these networks.
    # This can be useful if a spurious/invalid ASN appears
    # before the true "gateway" ASN.
    excluded = [None, "AS???"] + (excluded or [])

    reader = csv.DictReader(io.StringIO(str(output)))
    hops = {int(row["Hop"]): row for row in reader}

    if not hops:
        print("No response from MTR")
        return

    # (current asn, first TTL where it appeared)
    curr_asn = (None, 0)
    max_ttl = max(hops.keys())

    for ttl in range(1, max_ttl + 1):
        print("{}: {}".format(ttl, hops.get(ttl, {}).get("Asn", "*")))

    for ttl in range(min_ttl, max_ttl + 1):
        asn = hops.get(ttl, {}).get("Asn")
        if asn not in excluded:
            if not curr_asn[0]:
                curr_asn = (asn, ttl)
            elif curr_asn[0] != asn:
                curr_asn = (asn, ttl)
                break

    return max(min_ttl, curr_asn[1])


def find_exit_ttl_with_mtr(
    destination: str, min_ttl: int, *, excluded: Optional[List[str]] = None
) -> Optional[int]:
    """Find the first TTL which is not in the source AS."""
    print(f"Finding exit TTL towards {destination}...")
    output = mtr(
        destination,
        aslookup=True,
        csv=True,
        gracetime=1,
        no_dns=True,
        report_cycles=1,
    )
    return find_exit_ttl_from_output(output=output, min_ttl=min_ttl, excluded=excluded)
