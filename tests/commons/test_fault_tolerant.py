import pytest

from iris.commons.settings import fault_tolerant


class UnreliableService:
    def __init__(self, settings, logger):
        self.logger = logger
        self.settings = settings
        self.has_failed = False

    @fault_tolerant
    def do(self):
        if not self.has_failed:
            self.has_failed = True
            raise RuntimeError
        return True

    @fault_tolerant
    async def ado(self):
        return self.do()


async def test_decorator_disabled(settings, logger):
    settings.RETRY_TIMEOUT = -1
    service = UnreliableService(settings, logger)
    with pytest.raises(RuntimeError):
        service.do()


async def test_decorator_disabled_async(settings, logger):
    settings.RETRY_TIMEOUT = -1
    service = UnreliableService(settings, logger)
    with pytest.raises(RuntimeError):
        await service.ado()


async def test_decorator(settings, logger):
    settings.RETRY_TIMEOUT = 1
    settings.RETRY_TIMEOUT_RANDOM_MAX = 1
    service = UnreliableService(settings, logger)
    assert service.do()


async def test_decorator_async(settings, logger):
    settings.RETRY_TIMEOUT = 1
    settings.RETRY_TIMEOUT_RANDOM_MAX = 1
    service = UnreliableService(settings, logger)
    assert await service.ado()
