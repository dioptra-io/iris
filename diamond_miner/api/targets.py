"""Targets operations."""

import aioboto3

from fastapi import APIRouter, BackgroundTasks, UploadFile, File, status, HTTPException
from diamond_miner.commons.settings import Settings

router = APIRouter()
settings = Settings()

aws_settings = {
    "aws_access_key_id": settings.AWS_ACCESS_KEY_ID,
    "aws_secret_access_key": settings.AWS_SECRET_ACCESS_KEY,
    "endpoint_url": settings.AWS_S3_HOST,
    "region_name": settings.AWS_REGION_NAME,
}


@router.get("/")
async def get_targets():
    response = []
    async with aioboto3.resource("s3", **aws_settings) as s3:
        bucket = await s3.Bucket(settings.AWS_S3_TARGETS_BUCKET_NAME)
        async for file_object in bucket.objects.all():
            file_size = await file_object.size
            last_modified = await file_object.last_modified
            response.append(
                {
                    "key": file_object.key,
                    "size": file_size,
                    "last_modified": last_modified,
                }
            )
    return response


@router.get("/{key}")
async def get_target_by_key(key: str):
    async with aioboto3.client("s3", **aws_settings) as s3:
        try:
            file_object = await s3.get_object(
                Bucket=settings.AWS_S3_TARGETS_BUCKET_NAME, Key=key
            )
            async with file_object["Body"] as stream:
                await stream.read()
        except Exception:
            raise HTTPException(status_code=404, detail="File object not found")

        return {
            "key": key,
            "size": int(
                file_object["ResponseMetadata"]["HTTPHeaders"]["content-length"]
            ),
            "last_modified": file_object["ResponseMetadata"]["HTTPHeaders"][
                "last-modified"
            ],
        }


async def upload_targets_file(targets_file):
    """Upload targets file asynchronously."""
    async with aioboto3.client("s3", **aws_settings) as s3:
        await s3.upload_fileobj(
            targets_file.file,
            settings.AWS_S3_TARGETS_BUCKET_NAME,
            targets_file.filename,
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
    async with aioboto3.client("s3", **aws_settings) as s3:
        try:
            file_object = await s3.get_object(
                Bucket=settings.AWS_S3_TARGETS_BUCKET_NAME, Key=key
            )
            async with file_object["Body"] as stream:
                await stream.read()
        except Exception:
            raise HTTPException(status_code=404, detail="File object not found")

        response = await s3.delete_object(
            Bucket=settings.AWS_S3_TARGETS_BUCKET_NAME, Key=key
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] != 204:
            raise HTTPException(
                status_code=500, detail="Error while removing file object"
            )
        return {"key": key, "action": "delete"}
