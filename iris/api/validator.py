from diamond_miner.generators.standalone import count_prefixes
from fastapi import HTTPException
from starlette import status

from iris.commons.models.diamond_miner import Tool
from iris.commons.models.user import UserDB
from iris.commons.storage import Storage


async def target_file_validator(
    storage: Storage,
    tool: Tool,
    user: UserDB,
    target_filename: str,
    prefix_len_v4: int,
    prefix_len_v6: int,
):
    """Validate the target file input."""
    # Check validation for "Probe" tool
    # The user must be admin and the target file must have the proper metadata
    if tool == Tool.Probes:
        # Verify that the target file exists on S3
        try:
            target_file = await storage.get_file_no_retry(
                storage.targets_bucket(str(user.id)),
                target_filename,
                retrieve_content=False,
            )
        except Exception:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Probe file not found"
            )

        # Check if the user is admin
        if not user.is_superuser:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Admin privileges required",
            )

        # Check if the metadata is correct
        if not target_file["metadata"] or not (
            target_file["metadata"].get("is_probes_file")
        ):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Target file specified is not a probe file",
            )
        return 0, 255

    # Verify that the target file exists on S3
    try:
        target_file = await storage.get_file_no_retry(
            storage.targets_bucket(str(user.id)),
            target_filename,
        )
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Target file not found"
        )

    # Check if the prefixes respect the tool prefix length
    try:
        count_prefixes(
            (p.split(",")[0].strip() for p in target_file["content"].split()),
            prefix_len_v4=prefix_len_v4,
            prefix_len_v6=prefix_len_v6,
        )
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Invalid prefixes length"
        )

    # Check protocol and min/max TTL
    global_min_ttl = 256
    global_max_ttl = 0
    for line in [p.strip() for p in target_file["content"].split()]:
        _, protocol, min_ttl, max_ttl, n_initial_flows = line.split(",")
        min_ttl, max_ttl = int(min_ttl), int(max_ttl)
        global_min_ttl = min(global_min_ttl, min_ttl)
        global_max_ttl = max(global_max_ttl, max_ttl)
        if tool == Tool.Ping and protocol == "udp":
            # Disabling UDP port scanning abilities
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Tool `ping` only accessible with ICMP protocol",
            )
    return global_min_ttl, global_max_ttl
