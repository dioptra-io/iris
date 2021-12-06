# 🏛️ Iris — A resilient internet-scale measurement system

[![Python Code Quality](https://github.com/dioptra-io/iris/actions/workflows/quality.yml/badge.svg)](https://github.com/dioptra-io/iris/actions/workflows/quality.yml)
[![Coverage](https://img.shields.io/codecov/c/github/dioptra-io/iris?logo=codecov&logoColor=white&token=TC1WVMZORG)](https://app.codecov.io/gh/dioptra-io/iris)

Iris is a system to coordinate complex measurements from multiple vantage points.

Its main features are:
- Handle multi-round measurements, such as [diamond-miner](https://github.com/dioptra-io/diamond-miner) IP tracing measurements.
- Handle both centralized computation on a powerful server, and distributed probing on smaller agents.
- Can tolerate the temporary loss of agents, database or control-plane.

To start using Iris, please visit our [website](https://iris.dioptra.io)!

## Deployment

You can set up a production-ready system to orchestrate multiple vantage points from a dedicated API.

We provide a [docker-compose.yml](docker-compose.yml) file to set up Iris locally. Feel free to adapt it with your own configurations.
Don't forget to change the default passwords before pushing it to production!

Then, simply run the stack with docker-compose:
```bash
docker-compose up -d --build
```

## Publications

```
```

## Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for more information about how to contribute to this project.
