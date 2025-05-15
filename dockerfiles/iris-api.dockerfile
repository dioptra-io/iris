FROM docker.io/library/ubuntu:22.04 AS builder
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install --no-install-recommends --yes \
        build-essential \
        libpq-dev \
        python3-dev \
        python3-pip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip3 install --no-cache-dir poetry
RUN poetry config virtualenvs.in-project true

COPY pyproject.toml pyproject.toml
COPY poetry.lock poetry.lock

RUN poetry install --no-root \
    && rm -rf /root/.cache/*

FROM docker.io/library/ubuntu:22.04
LABEL maintainer="Matthieu Gouel <matthieu.gouel@lip6.fr>"
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install --no-install-recommends --yes \
        ca-certificates \
        libpq5 \
        python3 \
        tzdata \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY alembic.ini alembic.ini
COPY alembic alembic
COPY iris iris
COPY --from=builder /app/.venv .venv

EXPOSE 8000
CMD ["/app/.venv/bin/python3", "-m", "iris.api", "--access-logfile", "-", "--error-logfile", "-", "--bind", "0.0.0.0:8000"]
