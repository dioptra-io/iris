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
        curl \
        libpq5 \
        mtr \
        tzdata \
        zstd \
    && rm -rf /var/lib/apt/lists/*

RUN curl -L https://github.com/dioptra-io/caracal/releases/download/v0.15.3/caracal-linux-amd64 > /usr/bin/caracal \
    && chmod +x /usr/bin/caracal

WORKDIR /app

COPY iris iris
COPY --from=builder /app/.venv .venv
COPY statics/excluded_prefixes statics/excluded_prefixes

CMD ["/app/.venv/bin/python3", "-m", "iris.agent"]
