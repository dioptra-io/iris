# Concepts

## Agent

An agent performs measurements.
It receives a list of probes to send and uses [caracal](https://github.com/dioptra-io/caracal) to send the probes and capture the replies.
It is implemented as Python application that loops on a Redis queue and uploads the results to an S3 bucket.

The resources required to run an agent are minimal and depends mostly on the desired probing rate.
A minimum of 1 CPU and 512 MB of memory are required.
To achieve a rate of 100k packets per second, we recommend at-least 2 CPU.

If you're running an agent in the cloud, avoid burstable instances since the agent will exhaust the CPU credits very quickly and will become very slow.

## Measurement

A measurement is defined by a tool and a list of measurement agents.

## Measurement agent

A measurement agent is defined by an agent, a target list and tool parameters specific to the agent.

## Target list

A target list is a comma-delimited list of networks to probe.
Each line of the file must be like
```
target,protocol,min_ttl,max_ttl,n_initial_flows
```
where the target is a IPv4/IPv6 prefix or IPv4/IPv6 address.
The prococol can be `icmp`, `icmp6` or `udp`.
The file name must end with `.csv`.

For example:
```
0.0.0.0/0,udp,2,32,1
8.8.8.0/24,icmp,2,32,6
2001:4860:4860::8888,icmp6,2,32,6
```
If the prefix length is ignored, /24 or /128 is assumed.

Some tools offer the `prefix_len_v4` and `prefix_len_v6` parameters which allows to split the specified networks and keep the target list short.
For instance, if `prefix_len_v4=24`, then `0.0.0.0/0` will be split into the 16 millions networks `0.0.0.0/24,...,255.255.255.0/24`.

## Tool

A tool defines which probes should be sent based on a target list and the results of a previous measurement round.
Examples of such tools are Diamond-Miner, Yarrp or ping.

A better name would probably have been an _algorithm_, to avoid confusion with the actual probing tool that is used, [caracal](https://github.com/dioptra-io/caracal).

## Worker

A worker coordinates measurement agents.
It runs the tool to get the list of probes to send, it sends this list to the agent, and it waits for the results.
It is implemented as [Dramatiq](https://dramatiq.io) actors and uses Redis and S3 to exchange data with the agents.

The resources required to run a worker depends on the tool and on the number of concurrent measurement agents.
To run Diamond-Miner on `0.0.0.0/0` with a single agent, we recommend at-least 32 GB of memory and 8 CPUs.
