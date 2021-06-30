# Iris — A resilient internet-scale measurement system

[![Python Code Quality](https://github.com/dioptra-io/iris/actions/workflows/quality.yml/badge.svg)](https://github.com/dioptra-io/iris/actions/workflows/quality.yml)
[![Coverage](https://img.shields.io/codecov/c/github/dioptra-io/iris?logo=codecov&logoColor=white&token=TC1WVMZORG)](https://app.codecov.io/gh/dioptra-io/iris)

Iris is a system to coordinate complex measurements from multiple vantage points.

Its main features are:
- Handle multi-round measurements, such as [diamond-miner](https://github.com/dioptra-io/diamond-miner) IP tracing measurements.
- Handle both centralized computation on a powerful server, and distributed probing on smaller agents.
- Can tolerate the temporary loss of agents, database or control-plane.

## ⚡ Iris Standalone

The easiest way to run Iris from a single machine is to use the standalone version.

First, run a ClickHouse instance locally:
```bash
docker run -d -v $(pwd)/volumes/clickhouse:/var/lib/clickhouse -p 9000:9000 \
  yandex/clickhouse-server:latest
```

Then, simply run Iris standalone with a prefix list as an input:
```bash
docker run -i --network host -e DATABASE_HOST=127.0.0.1 \
  dioptraio/iris-standalone diamond-miner < prefixes.csv
```

The prefix list is a CSV file decribing for each line a prefix to probe with the protocol and the TTL range.
For instance:

```
8.8.8.0/24,icmp,2,32
8.8.8.0/24,udp,2,32
```

Note that a prefix can be a unique target (e.g., `8.8.8.8/32`) in the case of ping.

**TODO (when public)**
- [ ] Push iris-standalone to Docker Hub

## ✨ Iris Constellation

You can set up a production-ready system to orchestrate multiple vantage points from a dedicated API and monitor the operation through a monitoring stack.

We provide a [docker-compose.yml](docker-compose.yml) file to set up Iris Constellation locally. Feel free to adapt it with your own configurations. Don't forget to change the default passwords before pushing it to production!

Then, simply run the stack with docker-compose:
```bash
docker-compose up -d --build
```

With the configuration provided, the API is reachable via this link : [http://iris.docker.localhost/api/docs/](http://iris.docker.localhost/api/docs/). The API default credentials are `admin:admin`.

## Publications

## Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for more information about how to contribute to this project.
