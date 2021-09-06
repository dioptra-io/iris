FROM ubuntu:20.04
LABEL maintainer="Matthieu Gouel <matthieu.gouel@lip6.fr>"
ENV DEBIAN_FRONTEND=noninteractive

# TODO: Remove build-essential and python3-dev once
# we can push binary wheels on PyPI for diamond-miner.
RUN apt-get update \
    && apt-get install --no-install-recommends --yes \
        build-essential \
        ca-certificates \
        curl \
        python3-dev \
        python3-pip \
        tzdata \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir gunicorn uvicorn poetry
RUN poetry config virtualenvs.create false

COPY pyproject.toml pyproject.toml
COPY poetry.lock poetry.lock

RUN poetry install --no-root --no-dev --extras api \
    && rm -rf /root/.cache/*

COPY iris iris

EXPOSE 8000

HEALTHCHECK --interval=5s --timeout=5s --start-period=30s CMD curl --fail --max-time 5 localhost:8000/api/docs || exit 1

CMD ["gunicorn", "--access-logfile", "-", "--error-logfile", "-", "--bind", "0.0.0.0:8000", "--worker-class", "uvicorn.workers.UvicornWorker", "iris.api.main:app"]
