FROM docker.io/library/ubuntu:20.04 AS builder
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install --no-install-recommends --yes \
        build-essential \
        python3-dev \
        python3-pip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip3 install --no-cache-dir poetry
RUN poetry config virtualenvs.in-project true

COPY pyproject.toml pyproject.toml
COPY poetry.lock poetry.lock

# The API imports the worker (for dramatiq) so it also needs its dependencies.
RUN poetry install --no-root --no-dev --extras "api worker" \
    && rm -rf /root/.cache/*

FROM docker.io/library/ubuntu:20.04
LABEL maintainer="Matthieu Gouel <matthieu.gouel@lip6.fr>"
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install --no-install-recommends --yes \
        ca-certificates \
        python3 \
        tzdata \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY iris iris
COPY --from=builder /app/.venv .venv

EXPOSE 8000
CMD ["/app/.venv/bin/gunicorn", "--access-logfile", "-", "--error-logfile", "-", "--bind", "0.0.0.0:8000", "--worker-class", "uvicorn.workers.UvicornWorker", "iris.api.main:app"]
