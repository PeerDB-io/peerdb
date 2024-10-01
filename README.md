
<div align="center">

<img src="images/banner.jpg" alt="PeerDB Banner" width="512" />

#### Frustratingly simple ETL for Postgres

[![Workflow Status](https://github.com/PeerDB-io/peerdb/actions/workflows/ci.yml/badge.svg)](https://github.com/Peerdb-io/peerdb/actions/workflows/ci.yml)
[![ElV2 License](https://badgen.net/badge/License/Elv2/green?icon=github)](https://github.com/PeerDB-io/peerdb/blob/main/LICENSE.md)
[![Slack Community](https://img.shields.io/badge/slack-peerdb-brightgreen.svg?logo=slack)](https://slack.peerdb.io)

</div>

## PeerDB

At PeerDB, we are building a fast, simple and the most cost effective way to stream data from Postgres to Data Warehouses, Queues and Storage engines. If you are running Postgres at the heart of your data-stack and move data at scale from Postgres to any of the above targets, PeerDB can provide value.

We support different modes of streaming - log based (CDC), cursor based (timestamp or integer) and XMIN based. Performance wise, we are 10x faster than existing tools. Features wise, we support native Postgres features such as comprehensive set of data-types incl. jsonb/arrays/geospatial, efficiently streaming toast columns, schema changes and so on.

## Get started

```bash
git clone git@github.com:PeerDB-io/peerdb.git
cd peerdb

# Run docker containers: postgres as catalog, temporal, PeerDB server, PeerDB flow API + workers, PeerDB UI
# Requires docker and docker-compose installed: https://docs.docker.com/engine/install/
bash ./run-peerdb.sh
# OR for local development, images will be built locally.
# Requires docker, docker-compose as well as the buf compiler for protobuf generation
# https://buf.build/docs/installation
bash ./generate-protos.sh
bash ./dev-peerdb.sh

# connect to peerdb and query away (Use psql version >=14.0)
psql "port=9900 host=localhost password=peerdb"
```

<img src="images/peerdb-demo.gif" width="512" />

Follow this 5-minute [Quickstart Guide](https://docs.peerdb.io/quickstart#quickstart) to see PeerDB in action i.e. streaming data in real-time across stores.

## Why PeerDB

Current data tools prioritize a wide range of connectors, often neglecting to optimize for Postgres users. This can be problematic for those storing large amounts of data in Postgres and frequently transferring it. As a result, many resort to building custom pipelines when existing tools don't meet their needs. We've developed this project to provide a straightforward and reliable solution specifically for Postgres.

### Postgres-first Approach

PeerDB is an ETL/ELT tool built for PostgreSQL. We implement multiple Postgres native and infrastructural optimizations to provide a fast, reliable and a feature-rich experience for moving data in/out of PostgreSQL.

**For performance** -  we can parallelize initial load for a large table, still ensuring consistency. Syncing 100s of GB reduces from days to minutes. Our architecture is designed for real-time syncs and implements multiple logical replication related optimizations (tuning Postgres configs, parallel reading of slot etc.). This enables 10x faster Change Data Capture with data-freshness of a few 10s of seconds even at large throughputs (10k+ tps).

**For reliability**, we have mechanisms in place for fault tolerance - state management, automatic retries, handling idempotency and consistency and so on (<https://blog.peerdb.io/using-temporal-to-scale-data-synchronization-at-peerdb>) Configurable batching and parallelism prevent out of memory (OOMs) and crashes.

**From a feature richness standpoint**, we support efficient syncing of tables with large (TOAST) columns. We support multiple streaming modes - Log based (CDC) based, Query based streaming etc. We provide rich data-type mapping and plan to support every possible (incl. Custom types) that Postgres supports to the best extent possible on the target data-store.

#### **Postgres-compatible SQL interface to do ETL**

The Postgres-compatible SQL interface for ETL is unique to PeerDB and enables you to operate in a language you are familiar with. You can do ETL the same way you work with your databases.

You can use Postgres’ eco-system to manage your ETL —

1. Client tools like pgAdmin, psql to run SQL commands.
2. BI tools like Grafana, Tableau to visually monitor syncs and transforms.
3. Database migration and versioning tools like Flyway to manage your ETL.
4. Any language (Python, Go, Node.js etc) and Scheduler (AirFlow) for development.
5. And many more

## Status

We support multiple target connectors to move data from Postgres and a couple of source connectors to move data into Postgres. Check the status of connectors [here](https://docs.peerdb.io/sql/commands/supported-connectors)


## License

PeerDB is licensed under Elastic License 2.0 (ELv2). Please see the LICENSE file for additional information. If you have any licensing questions please email **<contact@peerdb.io>**
