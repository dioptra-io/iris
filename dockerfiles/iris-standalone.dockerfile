FROM docker.io/library/ubuntu:20.04
LABEL maintainer="Matthieu Gouel <matthieu.gouel@lip6.fr>"
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install --no-install-recommends --yes \
        apt-transport-https \
        binutils \
        build-essential \
        ca-certificates \
        curl \
        dirmngr \
        gnupg2 \
        mtr \
        python3-dev \
        python3-pip \
        tzdata \
        zstd \
    && rm -rf /var/lib/apt/lists/*

RUN curl -L https://github.com/dioptra-io/clickhouse-builds/releases/download/20211210/clickhouse.$(arch).zst | zstd > /usr/bin/clickhouse \
    && chmod +x /usr/bin/clickhouse

WORKDIR /app

RUN mkdir s3
RUN mkdir targets
RUN mkdir results

RUN pip3 install --no-cache-dir poetry
RUN poetry config virtualenvs.in-project true

COPY pyproject.toml pyproject.toml
COPY poetry.lock poetry.lock

RUN poetry install --no-dev --extras "agent api standalone worker" \
    && rm -rf /root/.cache/*

COPY iris iris
COPY statics/excluded_prefixes statics/excluded_prefixes
RUN mv iris/standalone/main.py main.py

ENTRYPOINT ["/app/.venv/bin/python3", "main.py"]
CMD ["--help"]
