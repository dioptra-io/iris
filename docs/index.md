# Introduction

Iris is a system to coordinate complex network measurements from multiple vantage points.
Think of it as a project similar to [CAIDA Ark](https://www.caida.org/projects/ark/) or [RIPE Atlas](https://atlas.ripe.net), with the following features:

- Fully open-source code.
- Handle multi-round measurements, such as [diamond-miner](https://github.com/dioptra-io/diamond-miner) IP tracing measurements.
- Handle both centralized computation on a powerful server, and distributed probing on smaller agents.
- Can tolerate the temporary loss of agents, database or control-plane components.

We offer a public instance of Iris, as well as public measurement data, on [iris.dioptra.io](https://iris.dioptra.io).
