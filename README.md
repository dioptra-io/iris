# 🕸️ Iris — An open-source internet measurement platform

[![Coverage](https://img.shields.io/codecov/c/github/dioptra-io/iris?logo=codecov&logoColor=white)](https://app.codecov.io/gh/dioptra-io/iris)
[![Tests](https://img.shields.io/github/actions/workflow/status/dioptra-io/iris/tests.yml?logo=github&label=tests)](https://github.com/dioptra-io/iris/actions/workflows/tests.yml)

Iris is a system to coordinate complex network measurements from multiple vantage points.
Think of it as a project similar to [CAIDA Ark](https://www.caida.org/projects/ark/) or [RIPE Atlas](https://atlas.ripe.net), with the following features:
- Fully open-source code.
- Handle multi-round measurements, such as [diamond-miner](https://github.com/dioptra-io/diamond-miner) IP tracing measurements.
- Handle both centralized computation on a powerful server, and distributed probing on smaller agents.
- Can tolerate the temporary loss of agents, database or control-plane components.

We offer a public instance of Iris, as well as public measurement data, on [iris.dioptra.io](https://iris.dioptra.io).

## 📖 Documentation

Please refer to the [documentation](https://dioptra-io.github.io/iris/) for more information on how to use Iris, deploy your own instance and contribute to the project.

## 📚 Publications

```bibtex
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
journal = {SIGCOMM Comput. Commun. Rev.},
month = {mar},
pages = {2–9},
numpages = {8},
keywords = {active internet measurements, internet topology}
}
```

## 🧑‍💻 Authors

Iris is developed and maintained by the [Dioptra group](https://dioptra.io) at [Sorbonne Université](https://www.sorbonne-universite.fr) in Paris, France.
