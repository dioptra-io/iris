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

RUN poetry install --no-root --no-dev --extras agent \
    && rm -rf /root/.cache/*

FROM docker.io/library/ubuntu:20.04
LABEL maintainer="Matthieu Gouel <matthieu.gouel@lip6.fr>"
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install --no-install-recommends --yes \
        ca-certificates \
        mtr \
        python3 \
        tzdata \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY iris iris
COPY iris/agent/main.py main.py
COPY --from=builder /app/.venv .venv

RUN mkdir targets
RUN mkdir results

COPY statics/excluded_prefixes statics/excluded_prefixes

EXPOSE 80
CMD ["/app/.venv/bin/python3", "-u", "main.py"]
