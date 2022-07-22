# HTTP API

Iris is meant to be controlled through an HTTP API.
The API is documented with Swagger on the `/docs` endpoint.
A publicly hosted documentation of the API is available at [https://api.iris.dioptra.io/docs/](https://api.iris.dioptra.io/docs/).

On this page we document specific tips for using the API.


## Registering

how to get an account: call /users/register or use website.
Must sign license in both cases.

### On iris.dioptra.io

### On a private instance

## iris-client

Based on httpx.

```bash
pip install dioptra-iris-client
```

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

## pych-client

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
