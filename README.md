# Iris

[![Python Code Quality](https://github.com/dioptra-io/iris/actions/workflows/quality.yml/badge.svg)](https://github.com/dioptra-io/iris/actions/workflows/quality.yml)
[![Coverage](https://img.shields.io/codecov/c/github/dioptra-io/iris?logo=codecov&logoColor=white&token=TC1WVMZORG)](https://app.codecov.io/gh/dioptra-io/iris)


Iris is a resilient internet-scale measurement system.


## ⚡ Iris Standalone

The simplest way to test Iris on a single machine is to use the standalone version.
It only needs a runniing Clickhouse database instance.

```
docker run -d -v $PWD/volumes/clickhouse:/var/lib/clickhouse -p 9000:9000 yandex/clickhouse-server:latest
```

Then simply run the Iris Standalone Docker image with a targets/prefixes list as input.

```
docker build -f dockerfiles/Dockerfile-standalone -t iris-standalone .
docker run -i \
--network host \
-e DATABASE_HOST=127.0.0.1 \
iris-standalone diamond-miner < resources/targets/prefixes.txt
```

## ✨ Iris Constellation

You can set up a production-ready system to orchestrate multiple vantage points from a dedicated API and monitor the operation through a monitoring stack.

```
docker-compose up -d --build
```


## Citation

## Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for more information about how to contribute to this project.
