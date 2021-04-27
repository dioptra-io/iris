"""Targets operations."""

import ipaddress

from fastapi import (
    APIRouter,
    BackgroundTasks,
    Depends,
    File,
    HTTPException,
    Query,
    Request,
    UploadFile,
    status,
)

from iris.api.pagination import ListPagination
from iris.api.schemas import (
    ExceptionResponse,
    TargetResponse,
    TargetsDeleteResponse,
    TargetsGetResponse,
    TargetsPostResponse,
)
from iris.api.security import get_current_active_user

router = APIRouter()


@router.get(
    "/", response_model=TargetsGetResponse, summary="Get all targets information"
)
async def get_targets(
    request: Request,
    offset: int = Query(0, ge=0),
    limit: int = Query(20, ge=0, le=200),
    user: str = Depends(get_current_active_user),
):
    """Get all targets lists information."""
    try:
        targets = await request.app.storage.get_all_files_no_retry(
            request.app.settings.AWS_S3_TARGETS_BUCKET_PREFIX + user["username"]
        )
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Bucket not found"
        )
    querier = ListPagination(targets, request, offset, limit)
    return await querier.query()


@router.get(
    "/{key}",
    response_model=TargetResponse,
    responses={404: {"model": ExceptionResponse}},
    summary="Get targets list information by key",
)
async def get_target_by_key(
    request: Request, key: str, user: str = Depends(get_current_active_user)
):
    """"Get a targets list information by key."""
    try:
        targets_file = await request.app.storage.get_file_no_retry(
            request.app.settings.AWS_S3_TARGETS_BUCKET_PREFIX + user["username"], key
        )
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="File object not found"
        )

    targets_file["content"] = [c.strip() for c in targets_file["content"].split()]

    return targets_file


async def verify_targets_file(targets_file):
    """Verify that a target file have a good structure."""
    # Check if file is empty
    targets_file.file.seek(0, 2)
    if targets_file.file.tell() == 0:
        return False
    targets_file.file.seek(0)

    # Check if all lines of the file is valid
    for line in targets_file.file.readlines():
        try:
            line_split = line.decode("utf-8").strip().split(",")
            try:
                ipaddress.ip_network(line_split[0])
            except ValueError:
                return False
            if line_split[1] not in ["icmp", "udp"]:
                return False
            try:
                if not (0 < int(line_split[2]) <= 255):
                    return False
                if not (0 < int(line_split[3]) <= 255):
                    return False
            except ValueError:
                return False
        except IndexError:
            return False
    targets_file.file.seek(0)
    return True


async def upload_targets_file(storage, target_bucket, targets_file):
    """Upload targets file asynchronously."""
    await storage.upload_file_no_retry(
        target_bucket,
        targets_file.filename,
        targets_file.file,
    )


@router.post(
    "/",
    status_code=status.HTTP_201_CREATED,
    response_model=TargetsPostResponse,
    summary="Upload a targets list",
)
async def post_target(
    request: Request,
    background_tasks: BackgroundTasks,
    targets_file: UploadFile = File(...),
    user: str = Depends(get_current_active_user),
):
    """Upload a targets list to object storage."""
    if not targets_file.filename.endswith(".csv"):
        raise HTTPException(
            status_code=status.HTTP_412_PRECONDITION_FAILED,
            detail="Bad targets file extension (.csv required)",
        )

    is_correct = await verify_targets_file(targets_file)
    if not is_correct:
        raise HTTPException(
            status_code=status.HTTP_412_PRECONDITION_FAILED,
            detail="Bad targets file structure",
        )

    target_bucket = request.app.settings.AWS_S3_TARGETS_BUCKET_PREFIX + user["username"]
    background_tasks.add_task(
        upload_targets_file, request.app.storage, target_bucket, targets_file
    )
    return {"key": targets_file.filename, "action": "upload"}


@router.delete(
    "/{key}",
    response_model=TargetsDeleteResponse,
    responses={404: {"model": ExceptionResponse}, 500: {"model": ExceptionResponse}},
    summary="Delete a targets list from object storage.",
)
async def delete_target_by_key(
    request: Request, key: str, user: str = Depends(get_current_active_user)
):
    """Delete a targets list from object storage."""
    try:
        response = await request.app.storage.delete_file_check_no_retry(
            request.app.settings.AWS_S3_TARGETS_BUCKET_PREFIX + user["username"], key
        )
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="File object not found"
        )

    if response["ResponseMetadata"]["HTTPStatusCode"] != 204:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error while removing file object",
        )
    return {"key": key, "action": "delete"}
