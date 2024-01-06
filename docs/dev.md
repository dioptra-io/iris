# Development

Here are some guidelines to make your life easier during the development process.

## Prerequisites

To develop on Iris you need a Python 3.10+ interpreter and Docker.

Iris services and their dependencies are hosted behind a Traefik reverse-proxy.
To be able to access them from your own machine, you need to add the following entries to [`/etc/hosts`](file:///etc/hosts):
```
127.0.0.1 api.docker.localhost
127.0.0.1 clickhouse.docker.localhost
127.0.0.1 minio.docker.localhost
127.0.0.1 minio-console.docker.localhost
127.0.0.1 postgres.docker.localhost
127.0.0.1 redis.docker.localhost
127.0.0.1 traefik.docker.localhost
```

You also need caracal in your $PATH if you intend to run Iris locally:
```bash
# Use caracal-macos-amd64 for macOS
curl -L https://github.com/dioptra-io/caracal/releases/download/v0.15.3/caracal-linux-amd64 > /usr/bin/caracal
chmod +x /usr/bin/caracal
```

## Running Iris

### Locally

```bash
# Create the virtual environment (only once)
poetry install
# Launch the external services
docker compose up --detach traefik clickhouse minio postgres redis
# Seed the database
poetry run alembic upgrade head
# Launch Iris
poetry run python -m iris.api
poetry run python -m iris.agent
poetry run python -m iris.worker
# Stop the external services
docker compose down
```

The API documentation will be available on http://127.0.0.1:8000/docs.

### On Docker

```bash
# Launch Iris and the external services
docker-compose up --detach --build
# Seed the database
poetry run alembic upgrade head
# Stop Iris and the external services
docker-compose down
```

The API documentation will be available on http://api.docker.localhost/docs.  
By default, the admin user is `admin@example.org` and the password is `admin`.

## Tests

```bash
# Excluding privileged tests
poetry run pytest
# Including privileged tests (`@superuser` decorator)
poetry run sudo pytest
# Do not delete test artifacts (see `conftest.py`)
export IRIS_TEST_CLEANUP=0
poetry run pytest
# Generate a coverage report
poetry run pytest --cov=iris --cov-report=html
```

## pre-commit

Please use the [pre-commit](https://pre-commit.com) hooks to format and lint the code before committing it.

```bash
poetry run pre-commit install
# On commit
git commit
# Manually
poetry run pre-commit run --all-files
```

## Release

Please use [bumpversion](https://pypi.org/project/bumpversion/0.6.0/) to conduct the releases.
The version bump will automatically create a new commit associated with a tag.
