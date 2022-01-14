import pytest


@pytest.mark.parametrize("origin", ["https://example.org", "http://localhost:8000"])
def test_cors_allowed_origin(make_client, make_user, origin):
    # https://fastapi.tiangolo.com/advanced/testing-events/?h=startup
    # https://github.com/encode/starlette/blob/master/tests/middleware/test_cors.py
    client = make_client()
    headers = {"Origin": origin}
    with client:
        r = client.options("/", headers=headers)
    assert r.headers["access-control-allow-credentials"] == "true"
    assert r.headers["access-control-allow-origin"] == origin


def test_cors_unallowed_origin(make_client, make_user):
    client = make_client()
    headers = {"Origin": "https://example.com"}
    with client:
        r = client.options("/", headers=headers)
    assert "access-control-allow-origin" not in r.headers
