def add_and_refresh(session, instances):
    session.add_all(instances)
    session.commit()
    for instance in instances:
        session.refresh(instance)


async def upload_file(storage, bucket, tmp_file):
    await storage.upload_file(
        bucket, tmp_file["name"], tmp_file["path"], tmp_file["metadata"]
    )
