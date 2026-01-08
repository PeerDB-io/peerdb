# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PeerDB is a high-performance ETL tool for streaming data from PostgreSQL, MySQL, and MongoDB to data warehouses (BigQuery, Snowflake, ClickHouse), queues (Kafka, EventHubs, PubSub), and storage (S3, GCS). It supports CDC (Change Data Capture), cursor-based, and XMIN-based replication modes.

## How Users Interact with PeerDB

Users create **peers** (database connections) and **mirrors** (data replication jobs) through:
1. **SQL interface** (port 9900) - `CREATE PEER`, `CREATE MIRROR` DDL commands via psql
2. **Web UI** - Dashboard for creating peers/mirrors and monitoring sync status
3. **gRPC API** - Programmatic access for automation

Example workflow:
```sql
-- Connect to PeerDB
psql "port=9900 host=localhost password=peerdb"

-- Create source and destination peers
CREATE PEER source_pg FROM POSTGRES WITH (...);
CREATE PEER dest_clickhouse FROM CLICKHOUSE WITH (...);

-- Create a CDC mirror to replicate data
CREATE MIRROR my_mirror FROM source_pg TO dest_clickhouse WITH (...);
```

## Architecture

For a deep dive into CDC internals, workflow pseudocode, and architecture diagrams, see [docs/architecture.md](docs/architecture.md).

### System Components

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Web UI    │     │  SQL (psql) │     │  gRPC API   │
│  (Next.js)  │     │  port 9900  │     │             │
└──────┬──────┘     └──────┬──────┘     └──────┬──────┘
       │                   │                   │
       │           ┌───────▼───────┐           │
       │           │    Nexus      │           │
       │           │(Rust pgwire)  │           │
       │           └───────┬───────┘           │
       │                   │                   │
       └───────────────────┼───────────────────┘
                           │
                    ┌──────▼──────┐
                    │  Flow API   │
                    │  (Go gRPC)  │
                    └──────┬──────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
       ┌──────▼──────┐     │     ┌──────▼──────┐
       │   Temporal  │◄────┘     │  Catalog DB │
       │   Server    │           │ (PostgreSQL)│
       └──────┬──────┘           └─────────────┘
              │
    ┌─────────┼─────────┐
    │         │         │
┌───▼───┐ ┌───▼───┐ ┌───▼───┐
│Worker │ │Worker │ │Snapshot│
│       │ │       │ │Worker  │
└───────┘ └───────┘ └────────┘
```

### Flow Component (Central Engine)

The `flow/` directory contains the core data movement engine with three main processes:

**1. API Server** (`flow/cmd/api.go`)
- gRPC server (default port 8110) + HTTP gateway (port 8111)
- Handles requests from UI and Nexus to create/manage peers and mirrors
- Starts Temporal workflows for data replication jobs
- Reads/writes mirror configuration to catalog database

**2. Worker** (`flow/cmd/worker.go`)
- Temporal worker that executes CDC and QRep workflows
- Listens on `peer-flow-task-queue`
- Runs activities: data extraction from sources, transformation, loading to destinations
- Maintains replication slot state, handles schema changes

**3. Snapshot Worker** (`flow/cmd/snapshot_worker.go`)
- Dedicated worker for initial table snapshots (backfill)
- Listens on `snapshot-flow-task-queue`
- Handles parallel snapshot partitioning for large tables

### Communication Flow

1. **User creates mirror** → Nexus/UI → Flow API
2. **Flow API** → Writes config to catalog DB → Starts Temporal workflow
3. **Temporal** → Schedules workflow on appropriate task queue
4. **Worker picks up workflow** → Executes activities (connect to source, read changes, write to destination)
5. **Worker** → Updates progress in catalog DB → Reports metrics

### Temporal Workflows

Key workflows in `flow/workflows/`:
- `CDCFlowWorkflow` - Continuous CDC replication (log-based)
- `QRepFlowWorkflow` - Query-based replication (cursor/watermark)
- `XminFlowWorkflow` - XMIN-based replication for Postgres
- `SnapshotFlowWorkflow` - Initial data backfill
- `SetupFlowWorkflow` - Mirror initialization (create replication slot, destination tables)

### Catalog Database

PostgreSQL database storing:
- Peer configurations (encrypted connection strings)
- Mirror definitions and state
- Replication progress (LSN positions, watermarks)
- Statistics and metrics

### Other Components

**nexus/** (Rust) - PostgreSQL-compatible SQL interface
- pgwire server exposing port 9900
- Parses PeerDB-specific DDL (`CREATE PEER`, `CREATE MIRROR`)
- Routes queries to appropriate peer or catalog
- Communicates with Flow API for mirror operations

**ui/** (Next.js/React) - Web dashboard
- Uses gRPC-generated types from `grpc_generated/`
- Calls Flow API directly for all operations

**protos/** - Protocol buffer definitions shared across components
- `flow.proto` - Data flow messages and configs
- `peers.proto` - Peer configuration types
- `route.proto` - Flow API service definition

## Development Commands

### Setup and Running

```bash
# Generate protos (requires buf: https://buf.build/docs/installation)
./generate-protos.sh

# Run with Docker (development - builds images locally)
./dev-peerdb.sh

# Run with Docker (production - pulls images)
./run-peerdb.sh

# Connect to PeerDB (psql >= 14.0)
psql "port=9900 host=localhost password=peerdb"
```

### Local Flow Development (Recommended for debugging)

Run infrastructure in Docker, flow components locally for fast iteration:

```bash
# 1. Start infrastructure only (catalog, temporal, minio)
docker compose -f docker-compose-infra.yml up -d

# 2. Apply catalog migrations
cat nexus/catalog/migrations/V{1..9}__*.sql nexus/catalog/migrations/V{10..49}__*.sql 2>/dev/null | \
  psql "host=localhost port=5432 user=postgres password=postgres dbname=postgres"

# 3. Load environment variables (from .env.local)
set -a && source .env.local && set +a

# 4. Build and run flow components locally
cd flow && go build -o peer-flow
./peer-flow api --port 8112 --gateway-port 8113  # Terminal 1
./peer-flow worker                                 # Terminal 2
./peer-flow snapshot-worker                        # Terminal 3 (optional)

# 5. Access services
# - Temporal UI: http://localhost:8085
# - MinIO Console: http://localhost:9002
# - Flow API: localhost:8112 (gRPC), localhost:8113 (HTTP)
```

**VS Code:** Use Run & Debug (F5) with "Flow API + Worker" - env vars loaded automatically.

### Flow (Go)

```bash
cd flow

# Build
go build -o peer-flow

# Run components
./peer-flow worker
./peer-flow snapshot-worker
./peer-flow api --port 8112 --gateway-port 8113

# Run all tests
go test ./...

# Run single test
go test -v -run TestName ./e2e/

# Run tests for specific connector
go test -v ./e2e/ -run 'Postgres'

# Lint (uses golangci-lint v2.5.0)
golangci-lint run --timeout=10m
```

### Nexus (Rust)

```bash
cd nexus

# Build
cargo build

# Check
cargo check

# Run tests (requires catalog DB on port 7132)
cargo test -- --test-threads=1

# Lint
cargo clippy -- -D warnings
```

### UI (Next.js)

```bash
cd ui

# Install dependencies
npm ci

# Development server
npm run dev

# Build
npm run build

# Lint
npm run lint          # ESLint
npm run format        # Prettier
```

## Testing Requirements

- Flow e2e tests require: PostgreSQL with PostGIS, ClickHouse, MongoDB, MySQL/MariaDB, MinIO, Temporal, Redis/Kafka
- Nexus tests require a catalog PostgreSQL database
- Tests run against multiple Postgres versions (14-18), MySQL variants (GTID, position-based, MariaDB), and ClickHouse versions (LTS, stable, latest)

## Key Environment Variables

```bash
# Catalog database
PEERDB_CATALOG_HOST, PEERDB_CATALOG_PORT, PEERDB_CATALOG_USER, PEERDB_CATALOG_PASSWORD, PEERDB_CATALOG_DATABASE

# Temporal
TEMPORAL_HOST_PORT, PEERDB_TEMPORAL_NAMESPACE

# Flow API
PEERDB_FLOW_SERVER_ADDRESS
```

## Code Style

- Go: Uses golangci-lint with extensive linters including gosec, gocritic, staticcheck. Max line length 144 chars. Import order: standard, github.com/PeerDB-io, third-party.
- Rust: Standard clippy with `-D warnings`
- TypeScript/React: ESLint + Prettier with max-warnings 0

## Proto Generation

After modifying `.proto` files in `protos/`:
```bash
./generate-protos.sh   # Generates Go code in flow/generated/ and TypeScript in ui/grpc_generated/
cd flow && go generate  # Generates typed gRPC handler wrapper
```
