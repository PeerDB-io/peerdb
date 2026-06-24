# Contributing to PeerDB

Thanks for your interest in contributing to PeerDB! Bug reports, feature requests, and pull requests are all welcome. If you have a question, feel free to drop by our [Slack](https://slack.peerdb.io/).

## Deprecated connectors

Several destination connectors (Snowflake, BigQuery, ElasticSearch, Kafka including Confluent and Redpanda, Azure Event Hubs, and Google Pub/Sub) are deprecated and no longer actively maintained. They remain fully functional, and no code is being removed.

If you depend on one of these connectors, see the [deprecated connectors migration guide](docs/deprecated-connectors.md) for how to pin to a release or fork the relevant connector code.
