![Infino Logo (Light)](docs/images/Infino_logo_light.png#gh-light-mode-only)
![Infino Logo (Dark)](docs/images/Infino_logo_dark.png#gh-dark-mode-only)

# Your next-gen observability solution
![Apache Logo] 
[![Github Commits](https://img.shields.io/github/commit-activity/m/infinohq/infino)](https://github.com/infinohq/infino/commits)
<a href="https://github.com/infinohq/infino/actions/workflows/post-merge-ci.yml">
  <img src="https://github.com/infinohq/infino/actions/workflows/post-merge-ci.yml/badge.svg?branch=main" alt="Status" >
</a>
[![Join Slack](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](https://infinohq.slack.com/join/shared_invite/zt-1tqqc0vsz-jF80cpkGy7aFsALQKggy8g#/shared-invite/email)


[Report Bug](https://github.com/infinohq/infino/issues/new?assignees=&labels=&template=bug_report.md) |
[Request Feature](https://github.com/infinohq/infino/issues/new?assignees=&labels=&template=feature_request.md)

<hr style="border:2px solid gray">

## What is Infino?

Infino is a scalable telemetry store designed for logs, metrics, and traces. Infino can function as a standalone observability solution or as the storage layer in your observability stack.

## Why Infino?
Telemetry data volumes are increasing exponentially yet there is no  purpose-built storage platform for telemetry. Most observability stacks are built on [ElasticSearch](https://github.com/elastic/elasticsearch-rs), [Clickhouse](https://github.com/ClickHouse/ClickHouse) or [Prometheus](https://github.com/prometheus/prometheus), which are powerful tools but are not built for modern telemetry data so the stacks become complex and expensive to manage. Infino's goal is to reduce the **cost** and **complexity** of observability with smart, high-performance storage for customers and vendors alike.

## How it works
To address **cost**, Infino focuses on performance ([**see benchmarks here**](benches/README.md)):

- **Index:** Append-only inverted index (more performant on telemetry data than general-purpose indexes like Lucene).
- **Time:** Efficient sharding and storage based on data recency.
- **Tools:** Rust core using SIMD instruction sets and telemetry-optimized compression.

To address **complexity**, Infino focuses on AI and automation:

- **Access:** NLP support + charts-on-demand for chat interfaces like Slack or Teams.
- **Management:** No schema, no labels, no master node + autoscaled everything.
- **Dashboards:** OSS dashboard support + SQL support for BI interfaces like Tableau or Sigma.
- **Analysis:** Hypeless LLMs + scalable search to accelerate your investigations.

![Architecture (Light)](docs/images/Infino_architecture_light.png#gh-light-mode-only)
![Architecture (Dark)](docs/images/Infino_architecture_dark.png#gh-dark-mode-only)

# What is the Infino plugin for OpenSearch?

An Infino Collection is a set of automanaged indexes in OpenSearch.  A collection consists internally of optimized indexes for both documents (Lucene) and telemetry (Infino) that are searchable together. Collections also have built-in AI co-pilots to do summarizations and root cause analysis.

Infino Collections are a great option for security and observability use cases, particularly if users do not want to manage and scale OpenSearch themselves. Infino Collections enable a much faster/cheaper/simpler version of OpenSearch for security and observability that accelerate root-cause analysis by combining code, tickets, telemetry, etc. 

In OpenSearch, a collection is represented by an index named Infino which contains stats about the collection and otherwise generally behaves as a normal index in OpenSearch. The caveat is that since collections are autoscaled and automanaged, the number of shards and replicas etc. for an Infino collection will always show as 1 and several index operations like split, merge, replicate, etc. are not honored as they don’t make sense for collections. 

Cluster requests work as normal but do not impact Infino Collections which are automanaged.

## Developer Docs
Read our repo documentation [here](https://infinohq.github.io/infino/doc/infino/index.html).

## Features
Note that we are still very much an alpha product but we have lots on the roadmap. Our development at the moment is focused on increasing the performance of the core engine to address **cost** but we are starting to add features to address **complexity**. 

#### Available now
 - Store logs and metrics
 - Ingest using [FluentBit](https://fluentbit.io/)
 - Query logs and metrics
 - Python client
 - LLM monitoring using [Langchain](https://github.com/langchain-ai/langchain)

#### Coming soon
- Dashboards
- SQL
- NLP
- Traces
- AI copilot

## Getting started

### Try it
For now, you need to build the repo. You will first need to:

- Install [Docker](https://docs.docker.com/engine/install/).
- Install [Rust toolchain](https://www.rust-lang.org/tools/install).
- Gradle build

### Examples

* [Integration with Fluentbit](examples/fluentbit/README.md) - Learn how to publish telemetry to Infino using FluentBit.
* [LLM monitoring with Langchain](examples/llm-monitoring-langchain/llm-monitoring-langchain.ipynb) - Discover how Infino's callback in Langchain can be used for monitoring requests in real-time. Also checkout the [Langchain <> Infino docs](https://python.langchain.com/docs/ecosystem/integrations/infino).
* [LLM monitoring with OpenAI](examples/llm-monitoring-openai/llm-monitoring-openai.ipynb) - See an example of monitoring OpenAI's services.
* [Using Infino's Python client](examples/python-client/rally-tracks.ipynb) - Get started with the [Python client](https://pyup.io/packages/pypi/infinopy/) for Infino and explore its functionalities.

</br>

Please file an issue if you face any problems or contact us directly if you want to discuss your use-case over virtual coffee.

## Contributions

Contributions are welcome and highly appreciated! To get started, check out our [repo docs](http://infinohq.github.io/infino/doc/infino/index.html) and the [contributing guidelines](CONTRIBUTING.md).

## Contact Us

Ping us on [Slack](https://infinohq.slack.com/join/shared_invite/zt-1tqqc0vsz-jF80cpkGy7aFsALQKggy8g#/shared-invite/email) or send us an email: ![Infino Logo (Light)](docs/images/Infino_email_light.svg#gh-light-mode-only)
![Infino Logo (Dark)](docs/images/Infino_email_dark.svg#gh-dark-mode-only).

## Contributors

A big thank you to the community for making Infino possible!

<a href="https://github.com/infinohq/infino/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=infinohq/infino" />
</a>

## License
Infino OpenSearch plugin is distributed under the Apache 2.0 license.


