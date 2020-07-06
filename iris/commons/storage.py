import aioboto3

from iris.commons.settings import CommonSettings

common_settings = CommonSettings()


class Storage(object):
    """AWS S3 object storage interface."""

    def __init__(self, settings=None):
        self.build_settings(settings)

    def build_settings(self, settings=None):
        if not settings:
            settings = common_settings

        self.settings = {
            "aws_access_key_id": settings.AWS_ACCESS_KEY_ID,
            "aws_secret_access_key": settings.AWS_SECRET_ACCESS_KEY,
            "endpoint_url": settings.AWS_S3_HOST,
            "region_name": settings.AWS_REGION_NAME,
        }

    async def get_ongoing_measurements(self):
        infrastructure_buckets = ["targets"]

        buckets = []
        async with aioboto3.client("s3", **self.settings) as s3:
            response = await s3.list_buckets()
        for bucket in response["Buckets"]:
            if bucket["Name"] in infrastructure_buckets:
                continue
            buckets.append(bucket["Name"])
        return buckets

    async def create_bucket(self, bucket):
        async with aioboto3.client("s3", **self.settings) as s3:
            await s3.create_bucket(Bucket=bucket)

    async def delete_bucket(self, bucket):
        async with aioboto3.client("s3", **self.settings) as s3:
            await s3.delete_bucket(Bucket=bucket)

    async def get_all_files(self, bucket):
        targets = []
        async with aioboto3.resource("s3", **self.settings) as s3:
            bucket = await s3.Bucket(bucket)
            async for file_object in bucket.objects.all():
                file_size = await file_object.size
                last_modified = str(await file_object.last_modified)
                targets.append(
                    {
                        "key": file_object.key,
                        "size": file_size,
                        "last_modified": last_modified,
                    }
                )
        return targets

    async def get_file(self, bucket, filename):
        async with aioboto3.client("s3", **self.settings) as s3:
            file_object = await s3.get_object(Bucket=bucket, Key=filename)
            async with file_object["Body"] as stream:
                await stream.read()

        return {
            "key": filename,
            "size": int(
                file_object["ResponseMetadata"]["HTTPHeaders"]["content-length"]
            ),
            "last_modified": file_object["ResponseMetadata"]["HTTPHeaders"][
                "last-modified"
            ],
        }

    async def upload_file(self, bucket, filename, fin):
        async with aioboto3.client("s3", **self.settings) as s3:
            await s3.upload_fileobj(
                fin, bucket, filename,
            )

    async def download_file(self, bucket, filename, output_path):
        async with aioboto3.client("s3", **self.settings) as s3:
            await s3.download_file(bucket, filename, output_path)

    async def delete_file_check(self, bucket, filename):
        async with aioboto3.client("s3", **self.settings) as s3:
            file_object = await s3.get_object(Bucket=bucket, Key=filename)
            async with file_object["Body"] as stream:
                await stream.read()

            return await s3.delete_object(Bucket=bucket, Key=filename)

    async def delete_file_no_check(self, bucket, filename):
        async with aioboto3.client("s3", **self.settings) as s3:
            return await s3.delete_object(Bucket=bucket, Key=filename)
