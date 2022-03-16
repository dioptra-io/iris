# 🕸️ Iris — An open-source internet measurement platform

[![Coverage](https://img.shields.io/codecov/c/github/dioptra-io/iris?logo=codecov&logoColor=white)](https://app.codecov.io/gh/dioptra-io/iris)
[![Docker](https://img.shields.io/github/workflow/status/dioptra-io/iris/Docker?logo=github&label=docker)](https://github.com/dioptra-io/iris/actions/workflows/docker.yml)
[![Tests](https://img.shields.io/github/workflow/status/dioptra-io/iris/Tests?logo=github&label=tests)](https://github.com/dioptra-io/iris/actions/workflows/quality.yml)

Iris is a system to coordinate complex network measurements from multiple vantage points.  
Think of it as a project similar to [CAIDA Ark](https://www.caida.org/projects/ark/) or [RIPE Atlas](https://atlas.ripe.net), with the following features:
- Fully open-source code.
- Handle multi-round measurements, such as [diamond-miner](https://github.com/dioptra-io/diamond-miner) IP tracing measurements.
- Handle both centralized computation on a powerful server, and distributed probing on smaller agents.
- Can tolerate the temporary loss of agents, database or control-plane components.

We offer a public instance of Iris, as well as public measurement data, on [iris.dioptra.io](https://iris.dioptra.io).

## 🚀 Deployment

See [`DEPLOYMENT.md`](DEPLOYMENT.md) for more information about how to deploy Iris on your own infrastructure.

## 📚 Publications

```
@article{10.1145/3523230.3523232,
author = {Gouel, Matthieu and Vermeulen, Kevin and Mouchet, Maxime and Rohrer, Justin P. and Fourmaux, Olivier and Friedman, Timur},
title = {Zeph &amp; Iris Map the Internet: A Resilient Reinforcement Learning Approach to Distributed IP Route Tracing},
year = {2022},
issue_date = {January 2022},
publisher = {Association for Computing Machinery},
address = {New York, NY, USA},
volume = {52},
number = {1},
issn = {0146-4833},
url = {https://doi.org/10.1145/3523230.3523232},
doi = {10.1145/3523230.3523232},
abstract = {We describe a new system for distributed tracing at the IP level of the routes that packets take through the IPv4 internet. Our Zeph algorithm coordinates route tracing efforts across agents at multiple vantage points, assigning to each agent a number of /24 destination prefixes in proportion to its probing budget and chosen according to a reinforcement learning heuristic that aims to maximize the number of multipath links discovered. Zeph runs on top of Iris, our fault tolerant system for orchestrating internet measurements across distributed agents of heterogeneous probing capacities. Iris is built around third party free open source software and modern containerization technology, thereby presenting a new model for assembling a resilient and maintainable internet measurement architecture. We show that carefully choosing the destinations to probe from which vantage point matters to optimize topology discovery and that a system can learn which assignment will maximize the overall discovery based on previous measurements. After 10 cycles of probing, Zeph is capable of discovering 2.4M nodes and 10M links in a cycle of 6 hours, when deployed on 5 Iris agents. This is at least 2 times more nodes and 5 times more links than other production systems for the same number of prefixes probed.},
journal = {SIGCOMM Comput. Commun. Rev.},
month = {mar},
pages = {2–9},
numpages = {8},
keywords = {active internet measurements, internet topology}
}
```

## ✏️ Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for more information about how to contribute to this project.


## 🧑‍💻 Authors

Iris is developed and maintained by the [Dioptra group](https://dioptra.io) at [Sorbonne Université](https://www.sorbonne-universite.fr) in Paris, France.
