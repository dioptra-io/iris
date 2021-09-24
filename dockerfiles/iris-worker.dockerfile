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

RUN poetry install --no-root --no-dev --extras worker \
    && rm -rf /root/.cache/*

FROM docker.io/library/ubuntu:20.04
LABEL maintainer="Matthieu Gouel <matthieu.gouel@lip6.fr>"
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install --no-install-recommends --yes \
        binutils \
        ca-certificates \
        curl \
        python3 \
        tzdata \
        zstd \
    && rm -rf /var/lib/apt/lists/*

RUN curl --location --output /usr/bin/clickhouse \
        "https://builds.clickhouse.tech/master/$(arch | sed s/x86_64/amd64/)/clickhouse" \
    && strip /usr/bin/clickhouse \
    && chmod +x /usr/bin/clickhouse

WORKDIR /app
COPY iris iris
COPY --from=builder /app/.venv .venv

RUN mkdir results

CMD ["/app/.venv/bin/dramatiq", "iris.worker.hook"]
