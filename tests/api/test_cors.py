def test_cors(make_client, make_user):
    # https://fastapi.tiangolo.com/advanced/testing-events/?h=startup
    # https://github.com/encode/starlette/blob/master/tests/middleware/test_cors.py
    client = make_client()
    headers = {"Origin": "https://example.org"}
    with client:
        r = client.options("/", headers=headers)
    assert r.headers["access-control-allow-credentials"] == "true"
    assert r.headers["access-control-allow-origin"] == "https://example.org"
