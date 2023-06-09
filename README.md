
<div align="center">

<img class="img-fluid" src="images/banner.jpg" alt="PeerDB Banner" width="512" />

#### Modern ETL in minutes, with SQL

[![Workflow Status](https://github.com/PEerDB-io/peerdb/actions/workflows/ci.yml/badge.svg)](https://github.com/Peerdb-io/peerdb/actions/workflows/ci.yml) [![ElV2 License](https://badgen.net/badge/License/Elv2/green?icon=github)](https://github.com/PeerDB-io/peerdb/blob/main/LICENSE.md) [![Slack Community](https://img.shields.io/badge/slack-peerdb-brightgreen.svg?logo=slack)](https://join.slack.com/t/peerdb-public/shared_invite/zt-1wo9jydev-EXInbMtCtpAKFFWdi7QvLQ)

</div>

## PeerDB

PeerDB is a Postgres-compatible SQL interface to seamlessly integrate multiple data-stores. It enables you to **sync**, **transform** and **query** data across your stores using simple SQL commands. PeerDB takes a datastore native approach in engineering — enabling 10x faster and a highly reliable ETL experience for you.

We are starting with Postgres, Snowflake and BigQuery as the supported data-stores and plan to expand to others based on user-feedback.

You can use PeerDB for any of the below use-cases:

1. Real-time sync (CDC) across stores.
2. Customized ETL across data-stores using SQL
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

<img class="img-fluid" src="images/peerdb_getstarted.jpg" alt="img-verification" width="450" height="272">

1. More details on adding PEERs available [here](https://docs.peerdb.io/sql/commands/create-peer)
2. More details on creating MIRRORs available [here](https://docs.peerdb.io/sql/commands/create-mirror)
3. Detailed documentation available [here](https://docs.peerdb.io).

## Why PeerDB

Existing ETL tools primarily focus on supporting a wide range of data-stores. However, they fall short in providing a rich experience for any two specific data-stores. This becomes evident when your workloads need scale or have demanding feature requirements. It is common for such users to try out these tools and fail – tools not meeting their performance and reliability SLAs or lacking the required features. Such users resort to developing their own in-house solutions, investing a lot of time and resources.

### Data-store nativity at it’s core, enabling scalable ETL

PeerDB takes a data-store first approach to ETL. It supports a set of highly adopted stores, implements multiple infrastructural and data-store native optimizations, providing a highly scalable and a feature-rich ETL experience. For example, in a sync from Postgres to BigQuery or Snowflake, PeerDB is 10 times faster than other tools. We are database experts and believe that an ETL tool should be datastore centric, than a hodge-podge of too many connectors.

#### **Postgres-compatible SQL interface to do ETL**

The Postgres-compatible SQL interface for ETL is unique to PeerDB and enables you to operate in a language you are familiar with. You can do ETL the same way you work with your databases.

You can use Postgres’ eco-system to manage your ETL —

1. Client tools like pgadmin, psql to run SQL commands.
2. BI tools like grafana, tableau to visually monitor syncs and transforms.
3. Database migration and versioning tools like Flyway to manage your ETL.
4. Any language (Python, Go, Node.JS etc) and Scheduler (AirFlow) for development.
5. And many more

## Status

Currently PeerDB is in development phase. We have not launched yet. Below tables captures different features and their state

### PeerDB Query

Query supported data-stores with a Postgres-compatible SQL interface

| Data-store | Support | Status |
| --- | --- | --- |
| BigQuery | SELECT commands | STABLE |
| Snowflake | SELECT commands | Beta |
| PostgreSQL | DML + SELECT commands | Beta |

### PeerDB MIRROR

Sync and transform data-from one store to another using CREATE MIRROR SQL command.

#### MIRROR for Streaming Changes (CDC)

Real-time syncing of data from source to target based on change-feed or CDC (logical decoding in the Postgres world)

| Feature | Source | Target | Status |
| --- | --- | --- | --- |
| CDC | PostgreSQL | BigQuery | Beta |
| CDC | PostgreSQL | Snowflake | Beta |
| CDC | PostgreSQL | Kafka | Beta |
| Initial Load | PostgreSQL | BigQuery | Coming Soon! |
| Initial Load | PostgreSQL | Snowflake | Coming Soon! |

#### MIRROR for SELECTs

Continuous syncing of data from source to target based on any SELECT query on the source. So this is basically a pre-transform - i.e. transform data on the source before syncing it to the target.

| Source | Target | Status |
| --- | --- | --- |
| PostgreSQL | BigQuery | Beta |
| PostgreSQL | Snowflake | Beta |
| PostgreSQL | S3 | Under development |

## License

PeerDB is licensed under Elastic License 2.0 (ELv2). Please see the LICENSE file for additional information. If you have any licensing questions please email **<founders@peerdb.io>**
