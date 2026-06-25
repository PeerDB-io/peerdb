# Deprecated connectors migration guide

The following **destination** connectors are deprecated and no longer actively maintained:

- Snowflake
- BigQuery (destination role only — see below)
- ElasticSearch
- Kafka (including Confluent and Redpanda variants)
- Azure Event Hubs
- Google Pub/Sub
- S3

These connectors remain **fully functional** in the current and all prior releases. No code is currently being removed, and they will **not** be automatically removed from existing deployments. The actively-maintained paths going forward are Postgres → ClickHouse, Postgres → ClickHouse Cloud, and Postgres → Postgres. Sources (Postgres, MySQL, MongoDB) are not affected by this deprecation.

> **BigQuery is deprecated only as a destination.** BigQuery remains a **supported source** for QRep mirrors and is not deprecated in that role. The migration guidance below applies to BigQuery's use as a *destination*.

If you rely on a deprecated destination, you have two supported options to keep using it: pin to a release that includes it, or fork the relevant connector code.

## How to pin to the last actively-maintained release

Deprecated connectors continue to ship in current and prior releases, so the simplest way to keep using one is to pin your deployment to a specific release rather than tracking the latest mainline.

- **Docker / docker-compose:** pin the PeerDB image tags to an explicit release tag instead of a moving tag like `latest`. In your `docker-compose.yml`, replace floating tags with the latest release tag that still includes the connector you depend on.
- **Helm chart:** pin the chart version and the image tags in your `values.yaml` to that same release.

Pin to the latest release tag before any future removal so that you have the most recent fixes while retaining the connector. Once pinned, validate your pipelines and avoid upgrading past a release that no longer carries the connector you need.

## How to fork

Because the connector code is still present, you can fork PeerDB and carry the relevant connector code yourself. The connectors are self-contained enough that forking and maintaining a single connector is practical:

1. Fork the repository.
2. Keep the connector's flow code, its UI form and helper, and (optionally) its end-to-end tests so you can continue validating it.
3. Rebase your fork periodically onto upstream `main` to pick up fixes to shared infrastructure.

The sections below list the exact code paths to carry for each deprecated connector.

### Snowflake

- `flow/connectors/snowflake/`
- `ui/components/PeerForms/SnowflakeForm.tsx`
- `ui/app/peers/create/[peerType]/helpers/sf.ts`
- `flow/e2e/snowflake*.go`

### BigQuery (destination only)

> Deprecated only as a **destination**. BigQuery is still a supported **source**, so keep this code if you use BigQuery as a source.

- `flow/connectors/bigquery/`
- `ui/components/PeerForms/BigqueryConfig.tsx`
- `ui/app/peers/create/[peerType]/helpers/bq.ts`
- `flow/e2e/bigquery.go`
- `flow/e2e/bigquery_qrep_test.go`
- `flow/e2e/bigquery_test.go`

### ElasticSearch

- `flow/connectors/elasticsearch/`
- `ui/components/PeerForms/ElasticsearchConfigForm.tsx`
- `ui/app/peers/create/[peerType]/helpers/es.ts`
- `flow/e2e/elasticsearch*.go`

### Azure Event Hubs

- `flow/connectors/eventhub/`
- `ui/components/PeerForms/Eventhubs/`
- `ui/app/peers/create/[peerType]/helpers/eh.ts`
- `ui/app/mirrors/create/cdc/eventhubsCallout.tsx`
- `flow/e2e/eventhub*.go`

### Kafka (including Confluent and Redpanda)

- `flow/connectors/kafka/`
- `ui/components/PeerForms/KafkaConfig.tsx`
- `ui/app/peers/create/[peerType]/helpers/ka.ts`
- `flow/e2e/kafka*.go`

### Google Pub/Sub

- `flow/connectors/pubsub/`
- `ui/components/PeerForms/PubSubConfig.tsx`
- `ui/app/peers/create/[peerType]/helpers/ps.ts`
- `flow/e2e/pubsub*.go`

### S3

- `flow/connectors/s3/`
- `ui/components/PeerForms/S3Form.tsx`
- `ui/app/peers/create/[peerType]/helpers/s3.ts`
- `flow/e2e/s3_*.go`
