# PeerDB

<p align="center">
<img class="img-fluid" src="images/logo-light-transparent_copy_2.png" alt="img-verification" width="100" height="100">
</p>


## Modern ETL in minutes, with SQL.

PeerDB is a Postgres-compatible SQL interface to seamlessly integrate multiple data-stores. It enables you to **sync**, **transform** and **query** data across your stores using simple SQL commands. It takes a datastore native approach in engineering — enabling 10x faster and a highly reliable ETL experience for you.

We are starting with Postgres, Snowflake and BigQuery as the supported data-stores. We will extend to other data-stores based on user feedback.

You can use PeerDB for any of the below use-cases:

1. Real-time sync (CDC) across stores.
2. Customized ETL across data-stores using SQL
3. Federated query workloads - Query multiple data-stores through a common SQL interface

## **Get started**

```jsx
git clone --recursive git@github.com:PeerDB-io/peerdb.git
cd peerdb

# Run docker containers: peerdb-server, postgres as catalog, temporal
docker compose --file stacks/dev.docker-compose.yml up --build

# connect to peerdb and query away
psql "port=9900 host=localhost password=peerdb"
```

Detailed documentation available [here](https://www.notion.so/f0e258f310dc4231ad35b6a210f7d4b1?pvs=21).

## Status

Currently PeerDB is in development phase. We have not launched yet. 

Below tables captures different features and their state

### **PeerDB Query**

Query supported data-stores with a Postgres-compatible SQL interface

| Data-store | Support | Status |
| --- | --- | --- |
| BigQuery | SELECT commands | STABLE |
| Snowflake | SELECT commands | Under development |
| PostgreSQL | DML + SELECT commands | Under development |

### **PeerDB MIRROR**

Sync and transform data-from one store to another using CREATE MIRROR SQL command.

**MIRROR for Streaming Changes (CDC)**

Real-time syncing of data from source to target based on change-feed or CDC (logical decoding in the Postgres world)

| Feature | Source | Target | Status |
| --- | --- | --- | --- |
| CDC | PostgreSQL | BigQuery | Under development |
| CDC | PostgreSQL | Snowflake | Under development |
| Initial Load | PostgreSQL | BigQuery | Coming Soon! |
| Initial Load | PostgreSQL | Snowflake | Coming Soon! |

**MIRROR for SELECTs**

Continuous syncing of data from source to target based on any SELECT query on the source. So this is basically 

| Source | Target | Status |
| --- | --- | --- |
| PostgreSQL | BigQuery | Under development |
| PostgreSQL | Snowflake | Under development |

## **Why PeerDB?**

Existing ETL tools primarily focus on supporting a wide range of data-stores. However, they fall short in providing a rich experience for any two specific data-stores. This becomes evident when your workloads need scale or have demanding feature requirements. It is common for such users to try out these tools and fail – tools not meeting their performance and reliability SLAs or lacking the required features. Such users resort to developing their own in-house solutions, investing a lot of time and resources.

### **Data-store nativity at it’s core, enabling scalable ETL**

PeerDB takes a data-store first approach to ETL. It supports a set of highly adopted stores, implements multiple infrastructural and data-store native optimizations, to provide a highly scalable and feature-rich ETL experience for you. For example, in a sync from Postgres to BigQuery or Snowflake, PeerDB is 10 times faster than other tools.

### **Postgres-compatible SQL interface to do ETL**

The Postgres-compatible interface for ETL is unique to PeerDB and enables you to operate in a language they are familiar with. The idea is that you do ETL the same way you work with your databases. 

You can use Postgres’ eco-system to manage your ETL — 

1. Client tools like pgadmin, psql to build ETL pipelines.
2. BI tools like grafana, tableau to visually monitor syncs and transforms.
3. Database migration and versioning tools like Flyway to manage your ETL.
4. Any language (Python, Go, Node.JS etc) for development etc.

## License

PeerDB is licensed under ELv2. Please see the LICENSE file for additional information. If you have any licensing questions please email **founders@peerdb.io**