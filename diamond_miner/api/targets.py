"""Targets operations."""

from fastapi import APIRouter, BackgroundTasks, UploadFile, File, status, HTTPException
from diamond_miner.api.settings import APISettings
from diamond_miner.commons.storage import Storage

router = APIRouter()
settings = APISettings()
storage = Storage()


@router.get("/")
async def get_targets():
    targets = await storage.get_all_files(settings.AWS_S3_TARGETS_BUCKET_NAME)
    return {"count": len(targets), "results": targets}


@router.get("/{key}")
async def get_target_by_key(key: str):
    try:
        target = await storage.get_file(settings.AWS_S3_TARGETS_BUCKET_NAME, key)
    except Exception:
        raise HTTPException(status_code=404, detail="File object not found")
    return target


async def upload_targets_file(targets_file):
    """Upload targets file asynchronously."""
    await storage.upload_file(
        settings.AWS_S3_TARGETS_BUCKET_NAME, targets_file.filename, targets_file.file
    )


@router.post("/", status_code=status.HTTP_201_CREATED)
async def post_target(
    background_tasks: BackgroundTasks, targets_file: UploadFile = File(...)
):
    """Upload a file."""
    background_tasks.add_task(upload_targets_file, targets_file)
    return {"key": targets_file.filename, "action": "upload"}


@router.delete("/{key}")
async def delete_target_by_key(key: str):
    """Delete a file."""
    try:
        response = await storage.delete_file_check(
            settings.AWS_S3_TARGETS_BUCKET_NAME, key
        )
    except Exception:
        raise HTTPException(status_code=404, detail="File object not found")

    if response["ResponseMetadata"]["HTTPStatusCode"] != 204:
        raise HTTPException(status_code=500, detail="Error while removing file object")
    return {"key": key, "action": "delete"}
