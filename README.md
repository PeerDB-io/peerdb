# Peerflake

> Peerflake is a community fork of [PeerDB](https://github.com/PeerDB-io/peerdb),
> originally created by the PeerDB team (now part of ClickHouse Inc.).
>
> This fork is maintained independently as a non-commercial open-source project,
> primarily to enable Snowflake destination testing via LocalStack's free OSS tier.
>
> Internal code, package names, and environment variables remain identical to
> upstream PeerDB to keep the fork easy to sync and to allow contributions to
> flow back upstream.

## What is Peerflake / PeerDB?

A fast, simple and cost effective way to stream data from Postgres to Data Warehouses, Queues and Storage engines. If you are running Postgres at the heart of your data-stack and move data at scale from Postgres to any of the above targets, Peerflake can provide value.

We support different modes of streaming — log based (CDC), cursor based (timestamp or integer) and XMIN based. Performance wise, we are 10x faster than existing tools. Features wise, we support native Postgres features such as comprehensive set of data-types incl. jsonb/arrays/geospatial, efficiently streaming TOAST columns, schema changes and so on.

For detailed upstream documentation, see the [PeerDB docs](https://docs.peerdb.io/introduction).

## Get started

```bash
git clone git@github.com:JGustavo0/peerflake.git
cd peerflake

# Run docker containers: postgres as catalog, temporal, server, flow API + workers, UI
# Requires docker and docker-compose installed: https://docs.docker.com/engine/install/
bash ./run-peerdb.sh
# OR for local development, images will be built locally.
# Requires docker, docker-compose as well as the buf compiler for protobuf generation
# https://buf.build/docs/installation
bash ./generate-protos.sh
bash ./dev-peerdb.sh

# connect and query away (Use psql version >=14.0)
psql "port=9900 host=localhost password=peerdb"
```

### **IMPORTANT: Ensuring ClickHouse Access to MinIO**

If your ClickHouse DB runs outside Docker (e.g., on VMs or ClickHouse Cloud), it may not have access to MinIO, which is used internally to stage files before loading them. Ensure ClickHouse has network access to MinIO.

Update `docker-compose.yml` and set `AWS_ENDPOINT_URL_S3` to MinIO's accessible IP:
```yaml
AWS_ENDPOINT_URL_S3: http://172.31.26.57:9001 # Change this to IP/host which is accessible by both Peerflake and ClickHouse
```

Rerun Docker Compose to apply changes. On AWS/GCP/Azure, also ensure the security group allows inbound access to MinIO.

## Local End to End testing

You can run locally the same end-to-end tests that our CI uses to validate changes, enabling fast iteration cycles during development.

For example:

```bash
cd flow
go clean -cache
go test -v -run TestGenericCH_MySQL ./e2e/
```

Or local debugging sessions.

These tests require both Peerflake services, source and destination stores to be running. We provide a local environment with all the necessary services and dependencies to run these tests.

This is done through [Tilt](https://tilt.dev/) orchestrated Docker compose.

To get the environment up you first need to specify the shared environment variables for both the test and the test environment in your local `.env` file. You can use the provided `.env.example` as a template: `cp .env.example .env `.

If a `.env` file is present in the project root, tests will automatically load it. Any variable defined in `.env` can be overridden by user-provided environment variables.

:memo: In the template, services URLs are set to `host.docker.internal`, which is the name for the default Docker gateway in Docker Desktop set-ups such as macOS and Windows. Using the default gateway address allows both test processes and services running inside Docker to access services on the host machine. In native Docker (Linux) this name is not resolved by default, you might replace it with the default gateway IP (e.g., `172.18.0.1`) or add a custom entry to your `/etc/hosts` file to resolve `host.docker.internal` to the appropriate IP address. e.g:

```bash
echo "172.18.0.1 host.docker.internal" | sudo tee -a /etc/hosts
```

Then you can just run:

```bash
./tilt.sh
```

And follow the status of the services and access logs through the Tilt UI at http://localhost:10352/. [Dozzle](https://dozzle.dev/) is also included at http://localhost:8118/, providing real-time container resource utilization metrics (CPU, memory) and log streaming for all running Docker containers.

<img width="1593" height="693" alt="image" src="https://github.com/user-attachments/assets/6c294dda-ca8f-45cc-b75c-11594118a641" />

Since `.env` is the environment configuration source of truth, tests automatically pick it up from the project root. For example:

```bash
go clean -cache; go test -v -run TestGenericCH_MySQL ./e2e/ # Some MySQL generic tests
```

### Running tests from Tilt

The Tilt setup includes pre-configured test launcher resources under the `e2e` label. These resources do not start automatically; instead, you can trigger them on demand from the Tilt UI at http://localhost:10352/.

<img width="964" height="265" alt="image" src="https://github.com/user-attachments/assets/04ecad65-5d60-44c3-96ad-d285b2706545" />

Available test launchers:

- **e2e_postgres** -- Postgres to ClickHouse generic tests (`TestGenericCH_PG`)
- **e2e_mysql-gtid** -- MySQL GTID to ClickHouse generic tests (`TestGenericCH_MySQL`)
- **e2e_mysql-pos** -- MySQL File-Pos to ClickHouse generic tests (`TestGenericCH_MySQL`)
- **e2e_mariadb** -- MariaDB to ClickHouse generic tests (`TestGenericCH_MySQL`)
- **e2e_mongodb** -- MongoDB to ClickHouse test suite (`TestMongoClickhouseSuite`)

Each launcher automatically depends on the required services and provisioning steps, so Tilt will ensure all prerequisites are running before executing the tests. To trigger a test, click the resource in the Tilt UI and press the trigger (play) button.

### Manual DataStore Initialization

DataStore resources (ClickHouse, MongoDB, MySQL variants, PostgreSQL) use **manual initialization** (`auto_init=False`) to conserve resources. This allows you to selectively start only the databases needed for your testing scenario.

![DataStore Resources](https://github.com/user-attachments/assets/970568c6-f783-4250-8899-45ec4e5aac9e)

To start a datastore:
1. Navigate to the Tilt UI at http://localhost:10352/
2. Locate the desired DataStore resource
3. Click the resource and press the play/trigger button

The provisioning step for each datastore will automatically execute once the datastore is running.

### Testing Multiple MySQL Flavors

The Tilt environment now supports running tests against different MySQL replication modes and MariaDB simultaneously without requiring file edits. Each MySQL variant has its own dedicated port and test launcher:

- **mysql-gtid** (port 3306): MySQL 9.5 with GTID replication
- **mysql-pos** (port 3307): MySQL 5.7 with file-position replication (ARM-compatible using `biarms/mysql` image)
- **mariadb** (port 3308): MariaDB with binary log replication

This allows comprehensive testing across different MySQL configurations in a single development session.

### Named Volumes and Cleanup

All ancillary services now use Docker Compose named volumes for data persistence. This enables:
- Data analysis after environment teardown
- Faster restarts by preserving database state
- Easy cleanup via the Tilt UI

To clean up unused volumes, use the **"Wipe ancillary volumes"** button in the Tilt UI navigation bar:

![Volume Cleanup Success](https://github.com/user-attachments/assets/ecec7edf-298b-4d1d-960b-40cf22e0d8f5)

This button removes all named volumes defined in `ancillary-docker-compose.yml` that are not currently in use. For example, if MongoDB is disabled, clicking this button will delete its data volume.

![Volume Cleanup Button](https://github.com/user-attachments/assets/6c06d4fe-dd15-4979-b10e-91d8ad50e307)

### Environment services versions

Data stores versions are extracted from `.github/workflows/flow.yml`, select the last row of the test matrix except for MySQL version which defaults to `9.5`.
This automatic extraction relies on the `yq` CLI; install the Go-based [`mikefarah/yq`](https://github.com/mikefarah/yq) version 4 or later so that local environment generation works correctly.
You can specify different versions in the local `.env` file to override them as follows:

```bash
MYSQL_VERSION=8.0
MONGODB_VERSION=4.4
CLICKHOUSE_VERSION=21.8
```

## Upstream

This is a fork of [PeerDB](https://github.com/PeerDB-io/peerdb). See [FORK.md](FORK.md) for fork policy and sync workflow.

For upstream docs, visit [docs.peerdb.io](https://docs.peerdb.io/introduction). For upstream community, join the [PeerDB Slack](https://slack.peerdb.io/).

## License

Peerflake is licensed under GNU Affero General Public License v3.0 (AGPLv3), the same license as the original PeerDB project. Please see the [LICENSE](LICENSE) file for additional information.
