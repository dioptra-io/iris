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

RUN poetry install --no-root --no-dev \
    && rm -rf /root/.cache/*

FROM docker.io/library/ubuntu:22.04
LABEL maintainer="Matthieu Gouel <matthieu.gouel@lip6.fr>"
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install --no-install-recommends --yes \
        ca-certificates \
        curl \
        libpq5 \
        mtr \
        python3 \
        tzdata \
    && rm -rf /var/lib/apt/lists/*

RUN curl -L https://github.com/dioptra-io/caracal/releases/download/v0.15.1/caracal-linux-amd64 > /usr/bin/caracal \
    && chmod +x /usr/bin/caracal

WORKDIR /app

COPY iris iris
COPY --from=builder /app/.venv .venv
COPY statics/excluded_prefixes statics/excluded_prefixes

CMD ["/app/.venv/bin/python3", "-m", "iris.agent"]
