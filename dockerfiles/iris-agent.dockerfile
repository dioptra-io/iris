FROM ubuntu:20.04
LABEL maintainer="Matthieu Gouel <matthieu.gouel@lip6.fr>"
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install --no-install-recommends --yes \
        build-essential \
        ca-certificates \
        mtr \
        python3-dev \
        python3-pip \
        tzdata \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip3 install --no-cache-dir poetry==1.1.7
RUN poetry config virtualenvs.create false

COPY pyproject.toml pyproject.toml
COPY poetry.lock poetry.lock

RUN poetry install --no-root --no-dev --extras agent \
    && rm -rf /root/.cache/*

COPY iris iris
COPY iris/agent/main.py main.py

RUN mkdir targets
RUN mkdir results

COPY statics/excluded_prefixes statics/excluded_prefixes
COPY statics/index.html statics/index.html

EXPOSE 80

CMD ["sh", "-c", "python3 -m http.server -d statics/ 80 & python3 -u main.py"]
