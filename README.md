
<div align="center">

<img class="img-fluid" src="images/banner.jpg" alt="PeerDB Banner" width="512" />

#### Frustratingly simple ETL for Postgres

[![Workflow Status](https://github.com/PEerDB-io/peerdb/actions/workflows/ci.yml/badge.svg)](https://github.com/Peerdb-io/peerdb/actions/workflows/ci.yml) [![ElV2 License](https://badgen.net/badge/License/Elv2/green?icon=github)](https://github.com/PeerDB-io/peerdb/blob/main/LICENSE.md) [![Slack Community](https://img.shields.io/badge/slack-peerdb-brightgreen.svg?logo=slack)](https://join.slack.com/t/peerdb-public/shared_invite/zt-1wo9jydev-EXInbMtCtpAKFFWdi7QvLQ)

</div>

## PeerDB

PeerDB is a Postgres-first ETL/ELT platform that makes moving data in and out of Postgres fast and simple. It enables you to **sync**, **transform** and **query** data across your stores using simple SQL commands. We implement multiple Postgres native and infrastructural optimizations for 10x faster data-movement in and out of PostgreSQL.

You can use PeerDB for any of the below use-cases:

1. Real-time Change Data Capture from PostgreSQL.
2. Real-time Streaming of Query results across data-stores
3. Federated query workloads - Query multiple data-stores through a common SQL interface

## Get started

```bash
git clone --recursive git@github.com:PeerDB-io/peerdb.git
cd peerdb

# Run docker containers: peerdb-server, postgres as catalog, temporal
export COMPOSE_PROJECT_NAME=peerdb-stack
docker compose up

# connect to peerdb and query away
psql "port=9900 host=localhost password=peerdb"
```
Follow this 5-minute [Quickstart Guide](https://docs.peerdb.io/quickstart#quickstart) to see PeerDB in action i.e. streaming data in real-time across stores.

## Why PeerDB

Existing ETL/ELT tools primarily focus on supporting a wide range of connectors rather than providing a high quality experience for Postgres. This affects customers who run Postgres at the heart of their data-stack, storing at least 10s of GB of data and frequently moving data in and out of Postgres. It is common for such users to try existing ETL/ELT tools and fail. They spend significant time and resources to build in-house pipelines. This is the gap we want to bridge by building an ETL/ELT tool for Postgres, that just works!

### Postgres-first ETL/ELT

PeerDB is an ETL/ELT tool built for PostgreSQL. We implement multiple Postgres native and infrastructural optimizations to provide a fast, reliable and a feature-rich experience for moving data in/out of PostgreSQL. 

**For performance** -  we can parallelize initial load for a large table, still ensuring consistency. Syncing 100s of GB reduces from days to minutes. Our architecture is designed for real-time syncs and implements multiple logical replication related optimizations (tuning Postgres configs, parallel reading of slot etc.). This enables 10x faster Change Data Capture with data-freshness of a few 10s of seconds even at large throughputs (10k+ tps).

**For reliability**, we have mechanisms in place for fault tolerance - state management, automatic retries, handling idempotency and consistency and so on (https://blog.peerdb.io/using-temporal-to-scale-data-synchronization-at-peerdb) Configurable batching and parallelism prevent out of memory (OOMs) and crashes.

**From a feature richness standpoint**, we support efficient syncing of tables with large (TOAST) columns. We support multiple streaming modes - Log based (CDC) based, Query based streaming etc. We provide rich data-type mapping and plan to support every possible (incl. Custom types) that Postgres supports to the best extent possible on the target data-store.


#### **Postgres-compatible SQL interface to do ETL**

The Postgres-compatible SQL interface for ETL is unique to PeerDB and enables you to operate in a language you are familiar with. You can do ETL the same way you work with your databases.

You can use Postgres’ eco-system to manage your ETL —

1. Client tools like pgadmin, psql to run SQL commands.
2. BI tools like grafana, tableau to visually monitor syncs and transforms.
3. Database migration and versioning tools like Flyway to manage your ETL.
4. Any language (Python, Go, Node.JS etc) and Scheduler (AirFlow) for development.
5. And many more

## Status

We support multiple target connectors to move data from Postgres and a couple of source connectors to move data into Postgres. Check the status of connectors [here](https://docs.peerdb.io/sql/commands/supported-connectors)

## License

PeerDB is licensed under Elastic License 2.0 (ELv2). Please see the LICENSE file for additional information. If you have any licensing questions please email **<founders@peerdb.io>**
