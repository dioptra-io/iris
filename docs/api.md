# HTTP API

Iris is meant to be controlled through an HTTP API.
The API is documented with Swagger on the `/docs` endpoint.
A documentation of the API is publicly available at [https://api.iris.dioptra.io/docs/](https://api.iris.dioptra.io/docs/).

On this page we document specific tips for using the API.

## Registering

### On iris.dioptra.io

To register on Iris public instance, you must go through [iris.dioptra.io](https://iris.dioptra.io).
Upon registration, you will be invited to sign a license and send it to our team for approval.
Once your account is approved you will be able to access public Iris data.

### On a private instance

To register on a private instance, you can use the `/auth/register` endpoint:
```
curl -X POST \
    -H 'Content-Type: application/json' \
    -d '{"firstname": "First", "lastname": "Last", "email": "a@b.c", "password": "abc"}' \
    https://api.dev.iris.dioptra.io/auth/register
```

```json
{
  "id": "23ec99b5-f259-4732-b121-d012b686e37a",
  "email": "a@b.c",
  "is_active": true,
  "is_superuser": false,
  "is_verified": false,
  "firstname": "First",
  "lastname": "Last",
  "probing_enabled": false,
  "probing_limit": 1,
  "allow_tag_reserved": false,
  "allow_tag_public": false,
  "creation_time": "2022-08-17T14:24:22.495581"
}
```

An administrator can then set `is_verified` and `probing_enabled` to true by issuing a PATCH query against the `/users/:id` endpoint.

## iris-client

To make it easier to use the Iris API from Python code, you can use the [iris-client](https://github.com/dioptra-io/iris-client) library.
It is implemented on top of [httpx](https://github.com/encode/httpx) and has sync and async interfaces.

```bash
pip install dioptra-iris-client
```

To avoid specifying the credentials in the code, you can use environment variables, or a configuration file:

```json title="~/.config/iris/credentials.json"
{
    "base_url": "https://api.iris.dioptra.io",
    "username": "admin@example.org",
    "password": "admin"
}
```

```python
from iris_client import IrisClient

with IrisClient() as iris:
    iris.all("/measurements/", params={"tag": "collection:public", "only_mine": False})
    iris.post("/measurements/", json={"tool": "diamond-miner", "agents": "..."})
```

For more information, refer to the documentation of the library.

## pych-client

To access Iris data hosted on ClickHouse, you can use the [pych-client](https://github.com/dioptra-io/pych-client) library in combination with iris-client.

```bash
pip install pych-client
```

```python
from iris_client import IrisClient
from pych_client import ClickHouseClient

with IrisClient() as iris:
    services = iris.get("/users/me/services").json()
    with ClickHouseClient(**services["clickhouse"]) as clickhouse:
        print(clickhouse.json("SHOW TABLES"))

```
