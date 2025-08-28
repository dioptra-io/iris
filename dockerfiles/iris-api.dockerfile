FROM docker.io/library/python:3.10.18-bookworm AS builder
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install --no-install-recommends --yes \
        build-essential \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml pyproject.toml
COPY poetry.lock poetry.lock
RUN pip3 install --no-cache-dir poetry \
    && poetry config virtualenvs.in-project true \
    && poetry install --no-root \
    && rm -rf /root/.cache/*

FROM docker.io/library/python:3.10.18-bookworm
LABEL maintainer="Elena Nardi <elena.nardi@lip6.fr>"
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install --no-install-recommends --yes \
        ca-certificates \
        libpq5 \
        tzdata \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY alembic.ini alembic.ini
COPY alembic alembic
COPY iris iris
COPY --from=builder /app/.venv .venv

EXPOSE 8000
CMD ["/app/.venv/bin/python3", "-m", "iris.api", "--access-logfile", "-", "--error-logfile", "-", "--bind", "0.0.0.0:8000"]
