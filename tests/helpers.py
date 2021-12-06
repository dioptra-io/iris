import httpx


def async_mock(return_value):
    async def f(*args, **kwargs):
        return return_value

    return f


def async_wrap(f):
    async def wrapper(*args, **kwargs):
        return f(*args, **kwargs)

    return f


def fake_redis_factory(agent=None, measurement_state=None):
    def fake_redis():
        class FakeRedis:
            async def get_agents(*args, **kwargs):
                if agent:
                    return [agent]

            async def get_agents_by_uuid(*args, **kwargs):
                if agent:
                    return {agent.uuid: agent}

            async def get_agent_state(*args, **kwargs):
                if agent:
                    return agent.state

            async def get_agent_parameters(*args, **kwargs):
                if agent:
                    return {}

            async def check_agent(*args, **kwargs):
                return agent is not None

            async def get_measurement_state(*args, **kwargs):
                if measurement_state:
                    return measurement_state

            async def set_measurement_state(*args, **kwargs):
                pass

        return FakeRedis()

    return fake_redis


def fake_storage_factory(files):
    def fake_storage():
        class FakeStorage:
            def archive_bucket(*args, **kwargs) -> str:
                return "bucket"

            def targets_bucket(*args, **kwargs) -> str:
                return "bucket"

            async def get_all_files_no_retry(*args, **kwargs):
                return files

            async def get_file_no_retry(*args, **kwargs):
                if files:
                    return files[0]
                raise ValueError

            async def delete_file_check_no_retry(*args, **kwargs):
                if files:
                    return {"ResponseMetadata": {"HTTPStatusCode": 204}}
                raise ValueError

        return FakeStorage()

    return fake_storage


def override(api_client, old, new):
    if isinstance(api_client, httpx.AsyncClient):
        api_client._transport.app.dependency_overrides[old] = new
    else:
        api_client.app.dependency_overrides[old] = new
