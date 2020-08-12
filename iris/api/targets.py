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
from iris.api.security import authenticate
from iris.api.schemas import (
    ExceptionResponse,
    TargetResponse,
    TargetsGetResponse,
    TargetsPostResponse,
    TargetsDeleteResponse,
)
from iris.api.settings import APISettings
from iris.commons.storage import Storage

router = APIRouter()
settings = APISettings()
storage = Storage()


@router.get(
    "/", response_model=TargetsGetResponse, summary="Get all targets information"
)
async def get_targets(
    request: Request,
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=0, le=200),
    username: str = Depends(authenticate),
):
    """Get all targets lists information."""
    targets = await storage.get_all_files(settings.AWS_S3_TARGETS_BUCKET_NAME)
    querier = ListPagination(targets, request, offset, limit)
    return await querier.query()


@router.get(
    "/{key}",
    response_model=TargetResponse,
    responses={404: {"model": ExceptionResponse}},
    summary="Get targets list information by key",
)
async def get_target_by_key(key: str, username: str = Depends(authenticate)):
    """"Get a targets list information by key."""
    try:
        target = await storage.get_file(settings.AWS_S3_TARGETS_BUCKET_NAME, key)
    except Exception:
        raise HTTPException(status_code=404, detail="File object not found")
    return target


async def verify_targets_file(targets_file):
    """Verify that a target file have a good structure."""
    for line in targets_file.file.readlines():
        try:
            ipaddress.ip_address(line.decode("utf-8").strip())
        except ValueError:
            return False
    targets_file.file.seek(0)
    return True


async def upload_targets_file(targets_file):
    """Upload targets file asynchronously."""
    await storage.upload_file(
        settings.AWS_S3_TARGETS_BUCKET_NAME, targets_file.filename, targets_file.file
    )


@router.post(
    "/",
    status_code=status.HTTP_201_CREATED,
    response_model=TargetsPostResponse,
    summary="Upload a targets list",
)
async def post_target(
    background_tasks: BackgroundTasks,
    targets_file: UploadFile = File(...),
    username: str = Depends(authenticate),
):
    """Upload a targets list to object storage."""
    is_correct = await verify_targets_file(targets_file)
    if not is_correct:
        raise HTTPException(status_code=412, detail="Bad targets file structure")
    background_tasks.add_task(upload_targets_file, targets_file)
    return {"key": targets_file.filename, "action": "upload"}


@router.delete(
    "/{key}",
    response_model=TargetsDeleteResponse,
    responses={404: {"model": ExceptionResponse}, 500: {"model": ExceptionResponse}},
    summary="Delete a targets list from object storage.",
)
async def delete_target_by_key(key: str, username: str = Depends(authenticate)):
    """Delete a targets list from object storage."""
    try:
        response = await storage.delete_file_check(
            settings.AWS_S3_TARGETS_BUCKET_NAME, key
        )
    except Exception:
        raise HTTPException(status_code=404, detail="File object not found")

    if response["ResponseMetadata"]["HTTPStatusCode"] != 204:
        raise HTTPException(status_code=500, detail="Error while removing file object")
    return {"key": key, "action": "delete"}
