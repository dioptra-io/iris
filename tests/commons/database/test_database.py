import logging

import pytest

from iris.commons.database import Database


@pytest.mark.asyncio
async def test_database(common_settings):
    db = Database(common_settings, logging.getLogger(__name__))
    assert await db.create_database() is None
    assert await db.call("SELECT 'A', 1") == [("A", 1)]
