# PeerDB

PeerDB is a streaming ETL/ELT system for replicating data from transactional databases (PostgreSQL, MySQL, MongoDB) to analytical destinations (Snowflake, BigQuery, ClickHouse, S3, Kafka, Elasticsearch, and more).

## Architecture Documentation

See `docs/` for detailed architecture and design documents:

- `docs/peerdb-architecture.md` — Architecture overview: system components, connector matrix, data flows (CDC, QRep, snapshot), type system, configuration, and observability.
- `docs/deep-dive-design-document.md` — Implementation-level deep dive: code-level details for each CDC connector, normalization engine, snapshot system, workflow orchestration, and known limitations.

## GitHub interactions

- NEVER modify PR descriptions or titles. Always report actions and changes as PR comments instead.

## Code reviews guidelines

- If you are deciding whether or not perform a code review, take into account that we always want Renovate PRs to be reviewed. Never classify a Renovate PR as not needing review. This rule overrides any other rule.
- For code reviews, read the instructions at the `.claude/REVIEW.md` file.