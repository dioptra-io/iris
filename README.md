# Iris — A resilient internet-scale measurement system

[![Python Code Quality](https://github.com/dioptra-io/iris/actions/workflows/quality.yml/badge.svg)](https://github.com/dioptra-io/iris/actions/workflows/quality.yml)
[![Coverage](https://img.shields.io/codecov/c/github/dioptra-io/iris?logo=codecov&logoColor=white&token=TC1WVMZORG)](https://app.codecov.io/gh/dioptra-io/iris)

Iris is a system to coordinate complex measurements from multiple vantage points.
It can handle measurement tools requiring multiple rounds of probing to converge, such as [diamond-miner](https://github.com/dioptra-io/diamond-miner).

## ⚡ Iris Standalone

The easiest way to run Iris from a single machine is to use the standalone version.

First, run a ClickHouse instance locally:
```
docker run -d -v $(pwd)/volumes/clickhouse:/var/lib/clickhouse -p 9000:9000
  yandex/clickhouse-server:latest
```

Then, simply run Iris standalone with a target/prefix list as an input:
```
docker run -i --network host -e DATABASE_HOST=127.0.0.1 \
  dioptraio/iris-standalone diamond-miner < resources/targets/prefixes.txt
```

**TODO (when public)**
- [ ] Push iris-standalone to Docker Hub
- [ ] Describe the target/prefix list format

## ✨ Iris Constellation

You can set up a production-ready system to orchestrate multiple vantage points from a dedicated API and monitor the operation through a monitoring stack.

```
docker-compose up -d --build
```


## Citation

## Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for more information about how to contribute to this project.
