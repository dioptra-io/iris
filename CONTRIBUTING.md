# Contributing

Thanks for contributing ! Here is some guidelines to make your life easier during the development process.

## Preparation

You may need to add this to your hosts file:
```
127.0.0.1 api.docker.localhost
127.0.0.1 minio-console.docker.localhost
127.0.0.1 traefik.docker.localhost
```

## Run with Docker-Compose

For development purposes, you can setup a new local infrastructure like this.

```bash
docker-compose up --detach --build
# ...
docker-compose down
```

The API documentation will be available at `http://api.docker.localhost/docs`.

## Run locally

For easier debugging, you can run also run the code directly on your machine.

```bash
docker compose up --detach traefik clickhouse minio redis
poetry install --extras "api agent worker"
poetry run python -m iris.api
poetry run python -m iris.agent
poetry run python -m iris.worker
```

The API documentation will be available at `http://127.0.0.1:8000/docs`.

##
```bash
mkdir iris_data
alembic revision --autogenerate -m "migration description"
# Inspect file in alembic/versions/
alembic upgrade head
```

TODO: Document how to run with an IDE / debugger.
TODO: `env PYTHONDEVMODE=1 PYTHONTRACEMALLOC=1`

## Coding style

The code is formatted with black with its standard option.

Try to follow the rules, but prefer to make a MVP first, and the refactor.
Avoir overthinking and premature optimization.

####

https://sqlmodel.tiangolo.com/tutorial/fastapi/multiple-models/#inheritance-and-table-models

#### Prefer SQLModel imports over SQLAlchemy

#### Do not write un-needed doctests

```python
# Don't
def engine():
    """Return the engine."""
    ...
```

#### Pass the minimum number of information

#### Prefer summaries to docstrings for routes and do not write redundant docstrings
(Keep the *code entropy* high)
```python
# Don't
@router.get("/", summary="Get all measurements.", ...)
async def get_measurements(...):
    """Get all measurements."""
    ...

# Do
@router.get("/", summary="Get all measurements.", ...)
async def get_measurements(...):
    ...
```

## Syntax checking

You can check the syntax using flake8.

```bash
flake8 --ignore=E501,W503 iris
```

## Type checking

If you used annotations to do static Python type checking with mypy.

```bash
mypy iris
```

## Test coverage

You can run the coverage using pytest.

```bash
docker compose up --detach traefik clickhouse minio redis
pytest
# ...
docker compose down
```


## Release

It is recommended to use [bumpversion](https://pypi.org/project/bumpversion/0.6.0/) to conduct the releases.
The version bump will automatically create a new commit associated with a tag.
When pushed into Github, the tag will trigger a deployment workflow that will push the new version of Iris Agent into Docker Hub.
