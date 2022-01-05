"""Targets operations."""
from ipaddress import ip_address, ip_network

from fastapi import (
    APIRouter,
    Depends,
    File,
    HTTPException,
    Query,
    Request,
    UploadFile,
    status,
)

from iris.api.authentication import (
    assert_probing_enabled,
    current_superuser,
    current_verified_user,
)
from iris.api.dependencies import get_storage
from iris.commons.models.pagination import Paginated
from iris.commons.models.target import Target, TargetSummary
from iris.commons.models.user import UserDB
from iris.commons.storage import Storage

router = APIRouter()


@router.get(
    "/",
    response_model=Paginated[TargetSummary],
    summary="Get all target lists.",
)
async def get_targets(
    request: Request,
    offset: int = Query(0, ge=0),
    limit: int = Query(20, ge=0, le=200),
    user: UserDB = Depends(current_verified_user),
    storage: Storage = Depends(get_storage),
):
    """Get all target lists."""
    assert_probing_enabled(user)
    targets = await storage.get_all_files_no_retry(storage.targets_bucket(str(user.id)))
    summaries = [TargetSummary.from_s3(target) for target in targets]
    return Paginated.from_results(request.url, summaries, len(summaries), offset, limit)


@router.get(
    "/{key}",
    response_model=Target,
    summary="Get target list specified by key.",
)
async def get_target(
    key: str,
    with_content: bool = True,
    user: UserDB = Depends(current_verified_user),
    storage: Storage = Depends(get_storage),
):
    """Get a target list information by key."""
    assert_probing_enabled(user)
    target = await storage.get_file_no_retry(
        storage.targets_bucket(str(user.id)), key, retrieve_content=with_content
    )
    return Target.from_s3(target)


@router.post(
    "/",
    response_model=Target,
    status_code=status.HTTP_201_CREATED,
    summary="Upload a target list.",
    description="""
    Each line of the file must be like `target,protocol,min_ttl,max_ttl,n_initial_flows`
    where the target is a IPv4/IPv6 prefix or IPv4/IPv6 address.
    The prococol can be `icmp`, `icmp6` or `udp`.
    """,
)
async def post_target(
    target_file: UploadFile = File(...),
    user: UserDB = Depends(current_verified_user),
    storage: Storage = Depends(get_storage),
):
    """Upload a target list to object storage."""
    if not target_file.filename.endswith(".csv"):
        raise HTTPException(
            status_code=status.HTTP_412_PRECONDITION_FAILED,
            detail="Bad target file extension (.csv required)",
        )
    if not verify_target_file(target_file):
        raise HTTPException(
            status_code=status.HTTP_412_PRECONDITION_FAILED,
            detail="Bad target file structure",
        )
    await storage.upload_file_no_retry(
        storage.targets_bucket(str(user.id)), target_file.filename, target_file.file
    )
    return await get_target(
        key=target_file.filename, with_content=False, user=user, storage=storage
    )


@router.delete(
    "/{key}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a target list.",
)
async def delete_target(
    key: str,
    user: UserDB = Depends(current_verified_user),
    storage: Storage = Depends(get_storage),
):
    """Delete a target list from object storage."""
    assert_probing_enabled(user)
    response = await storage.delete_file_check_no_retry(
        storage.targets_bucket(str(user.id)), key
    )


@router.post(
    "/probes",
    response_model=Target,
    status_code=status.HTTP_201_CREATED,
    summary="Upload a probe list.",
    description="""
    Each line of the file must be like `dst_addr,src_port,dst_port,ttl,protocol`
    where the target is a IPv4/IPv6 prefix or IPv4/IPv6 address.
    The prococol can be `icmp`, `icmp6` or `udp`.
    """,
)
async def post_probes_target(
    target_file: UploadFile = File(...),
    user: UserDB = Depends(current_superuser),
    storage: Storage = Depends(get_storage),
):
    """Upload a probe list to object storage."""
    assert_probing_enabled(user)
    if not target_file.filename.endswith(".csv"):
        raise HTTPException(
            status_code=status.HTTP_412_PRECONDITION_FAILED,
            detail="Bad target file extension (.csv required)",
        )
    if not verify_probe_target_file(target_file):
        raise HTTPException(
            status_code=status.HTTP_412_PRECONDITION_FAILED,
            detail="Bad target file structure",
        )
    await storage.upload_file_no_retry(
        storage.targets_bucket(str(user.id)),
        target_file.filename,
        target_file.file,
        metadata={"is_probes_file": "True"},  # MinIO doesn't like bool type in metadata
    )
    return await get_target(
        key=target_file.filename, with_content=False, user=user, storage=storage
    )


def verify_target_file(target_file):
    """Verify that a target file have a good structure."""
    # Check if file is empty
    target_file.file.seek(0, 2)
    if target_file.file.tell() == 0:
        return False
    target_file.file.seek(0)

    # Check if all lines of the file is valid
    for line in target_file.file.readlines():
        try:
            line_split = line.decode("utf-8").strip().split(",")

            # Check if the prefix is valid
            ip_network(line_split[0])

            # Check if the protocol is supported
            if line_split[1] not in ["icmp", "icmp6", "udp"]:
                return False

            # Check the min TTL
            if not (0 < int(line_split[2]) <= 255):
                return False

            # Check the max TTL
            if not (0 < int(line_split[3]) <= 255):
                return False

        except Exception:
            return False

    target_file.file.seek(0)
    return True


def verify_probe_target_file(target_file):
    """Verify that a probe target file have a good structure."""
    # Check if file is empty
    target_file.file.seek(0, 2)
    if target_file.file.tell() == 0:
        return False
    target_file.file.seek(0)

    # Check if all lines of the file is valid
    for line in target_file.file.readlines():
        try:
            line_split = line.decode("utf-8").strip().split(",")

            # Check if the address is valid
            ip_address(line_split[0])

            # Check the source port
            if not (0 <= int(line_split[1]) <= 65535):
                return False

            # Check the destination port
            if not (0 <= int(line_split[2]) <= 65535):
                return False

            # Check the TTL
            if not (0 < int(line_split[3]) <= 255):
                return False

            # Check if the protocol is supported
            if line_split[4] not in ["icmp", "icmp6", "udp"]:
                return False

        except Exception:
            return False

    target_file.file.seek(0)
    return True
