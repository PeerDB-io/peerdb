# PeerDB CI → Praktika migration — TODO

Tracking what's deferred while we bring the `flow.yml` / `tilt-flow.yml` CI onto
Praktika. See `../PRAKTIKA_MIGRATION_ASSESSMENT.md` for the overall rationale.

Deferred work is split into:
- **A. Parity** — already exists in the GitHub Actions CI; we just need to port it.
- **B. Improvements** — new capabilities Praktika offers on top (no GHA equivalent).

## Done (Phase A)

- Runner pools: `arm_2cpu_4g` (c7g.large) and `amd_64cpu_128g` (c7i.16xlarge).
- Image bakes the toolchain: Rust+clippy, buf, protoc (arm+x86); Go, gotestsum,
  Tilt, libgeos-dev, postgresql-client-18, retry, zstd, yq (x86 only).
- Jobs: Rust Lint, Unit tests (no services), Test selection coverage, and the
  first Tilt e2e permutation `postgres-16_ch-lts`.
- `ci/scripts/tilt_flow.sh`: port of `tilt-flow.yml`'s orchestration bash
  (since reworked into the Python driver `ci/scripts/flow_test.py` +
  `tilt_env.py`, emitting a structured praktika Result).
- **A5 (full matrix):** data-driven in `ci/scripts/flow_matrix.py`, generated via
  `Job.parametrize`; the driver derives its target from `Info().job_name`. Cloud
  -only permutations (`postgres_other`, `bigquery_ch-*`) are disabled pending B4.
- **A1 (flow images once):** `ci/scripts/flow_images.py` builds the 3 images and
  publishes the `flow-images` S3 artifact; the matrix `requires` it and
  `flow_test.py` docker-loads it + sets `PEERDB_CI_PREBUILT_FLOW_IMAGES=1`.

---

## A. Parity — exists in GHA, port as-is

### A1. Build flow images once, share via artifact
GHA source: `flow.yml` `flow_images` job + `tilt-flow.yml`'s "Load prebuilt
flow images" step. Stops the matrix from rebuilding the 3 flow images per job.
- Builder job: `stacks/build-flow-images.sh` for `flow-api` / `flow-worker` /
  `flow-snapshot-worker`, `docker save | zstd` → Praktika artifact
  (`provides=["flow-images"]`).
- Matrix jobs `requires=` it; `tilt_flow.sh` does `zstd -d | docker load` and
  exports `PEERDB_CI_PREBUILT_FLOW_IMAGES=1` so the Tiltfile drops the builds.

### A2. Go build cache artifact (run-scoped)
GHA source: `flow.yml` `go_build_cache` job. Compile all flow test packages once
(`select-flow-tests.sh` over `./...`), pack the new `GOCACHE` entries → Praktika
artifact the matrix jobs restore. (Cross-run caching is an improvement — see B1.)

### A3. Artifact fetch mechanism
GHA source: `.github/scripts/fetch-artifact.sh` (GH artifact API + azcopy).
Replace with Praktika's native artifact download in `tilt_flow.sh` (the
`requires=` consumption from A1/A2 handles this).

### A4. Cloud credentials + o11y ingest
GHA source: `tilt-flow.yml` cred-setup steps + `.github/actions/ingest-test-results`.
Currently skipped (`FLOW_TESTS_RDS_IAM_AUTH_SKIP=true`, no creds). Re-home the
secrets into AWS SSM / Secrets Manager, scope the pool's `allowed_secrets`, and
export them in `tilt_flow.sh`:
- `FLOW_TESTS_AWS_ROLE_ARN`, `FLOW_TESTS_AWS_S3_BUCKET_NAME`
- `FLOW_TESTS_RDS_IAM_AUTH_{ASSUME_ROLE,CHAINED_ROLE,HOST_POSTGRES,HOST_POSTGRES_PROXY,USERNAME_POSTGRES,HOST_MYSQL,HOST_MYSQL_PROXY,USERNAME_MYSQL}`
- `GCP_GH_CI_PKEY` (→ `TEST_BQ_CREDS` + Cloud SQL ADC), `SNOWFLAKE_GH_CI_PKEY`,
  `GCS_CREDS`, `EH_CREDS`, `AZURE_{TENANT_ID,CLIENT_ID,CLIENT_SECRET,SUBSCRIPTION_ID}`
- `CI_O11Y_TARGET_{API_KEY_ID,API_KEY_SECRET,QUERY_ENDPOINT}`

Then re-enable the o11y ingest step and flip `FLOW_TESTS_RDS_IAM_AUTH_SKIP=false`.
(AWS specifically can be *simpler* than GHA — see B4.)

### A5. Full 21-permutation matrix
GHA source: `flow.yml` `flow_test` `strategy.matrix`. Generalize the single
`postgres-16_ch-lts` job to all 21 targets (postgres 16/17/18, mysql/mariadb
flavors, mongo 6/7/8, cockroach v24/25/26, bigquery × clickhouse lts/stable/latest)
via `Job.Config(...).parametrize(...)` over a Python table of
(packages, pkg, services, image pins, test_env).

### A6. Required-check gating
GHA source: `flow.yml` `gate` + `flow_tests_passed` jobs.
- Fork-PR gate: reproduce the `pull_request_target` + `external-contributor`
  manual-approval environment.
- Aggregation / branch-protection required check (`enable_merge_ready_status`).
- Drop `allow_failure=True` on the flow test job once reliably green.

---

## B. Improvements — Praktika capabilities on top (no GHA equivalent)

### B1. Persist the Go build cache *across* runs
GHA artifacts are run-scoped (A2 only avoids rework *within* one run). Praktika's
native CI cache (`enable_cache` + `digest_config` keyed on a content digest) can
carry the Go build cache between PRs/commits, so an unchanged package compiles
zero times on later runs. Supersedes A2 once trusted.

### B2. Docker pull-through proxy for ancillary DB images
The matrix cold-pulls clickhouse/mysql/mariadb/mongo/cockroach images on every
permutation. Praktika's docker proxy removes the per-job pull tax and the Docker
Hub rate-limit risk. No GHA equivalent in the current setup.

### B3. Dynamic / hook-driven matrix + test selection
`check-flow-test-selection.sh` / `select-flow-tests.sh` already compute what to
run; Praktika hooks can generate jobs dynamically from that, instead of the
static 21-way matrix (e.g. skip permutations whose packages are untouched).

### B4. Keyless cloud auth via OIDC / workload-identity federation
**Preferred over A4's "re-home the secret keys" path.** Store *zero* sensitive
cloud credentials: let the Praktika runner's AWS instance role be the single
identity, and federate from it into the other clouds. Besides dropping the
secret-store surface, this is what makes cloud tests safe to run on **fork PRs** —
a fork can exercise a scoped, keyless identity with no exfiltratable secret.

- **AWS** (RDS-IAM, ClickHouse S3-IAM, `s3_iam_role_test`): the runner already
  carries an instance role. Grant it (or let it `AssumeRole` a scoped test role)
  directly — no `FLOW_TESTS_AWS_ROLE_ARN` OIDC dance, no stored keys. Replaces
  the AWS half of A4.
- **GCP** (BigQuery / PubSub / GCS / Cloud SQL IAM): use Workload Identity
  Federation from the AWS instance role into a GCP service account — **the flow
  code already supports this** (`flow/connectors/utils/gcp_workload_identity.go`,
  `PEERDB_GCP_WORKLOAD_IDENTITY_*` in `.env.example`). Eliminates the
  `GCP_GH_CI_PKEY` service-account JSON key entirely.
- **Azure** (EventHubs): AAD workload-identity federation from the AWS role
  removes `AZURE_CLIENT_SECRET`.
- **Snowflake**: hardest — no AWS-federation story; keypair or OAuth still needs a
  stored secret. Lowest priority / may stay on A4.

Net: A4 shrinks to "Snowflake key + o11y keys" at most; everything else becomes
keyless. Fork-PR cloud coverage (A6) becomes feasible without leaking creds.

**Status:** the cloud-only permutations (`postgres_other` ×3 — BigQuery/Snowflake/
GCS/EventHubs/PubSub suites — and `bigquery_ch-*` ×3) are **disabled in
`ci/scripts/flow_matrix.py`** until this (or A4) lands; today they only fail with
`TEST_BQ_CREDS env var not set`.

### B5. Repo snapshot to S3 instead of per-job clone
`Settings.ENABLE_S3_REPO_SNAPSHOT` / `ENABLE_PR_EPHEMERAL_MERGE_COMMIT`: snapshot
the merge commit once and have downstream jobs restore it, instead of each job
re-cloning. Wall-clock win across a 21-job fan-out.

### B6. Consolidated report
Praktika's `praktika.html` report could absorb the per-combination test-results
view the o11y ingestion currently provides separately.

---

## Leave on GitHub Actions (not migrating)
CodeQL, renovate, release-drafter, `claude-*`, cleanup. GH-integrated and cheap.
