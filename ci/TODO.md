# PeerDB CI — Praktika migration TODO

Status: PR CI runs on Praktika (`ci/workflows/pull_request.py`); the GHA
workflows have their `pull_request` / `pull_request_target` triggers commented
out (files kept, non-PR triggers intact); no GHA workflow has a PR trigger left.
Claude PR review → native Praktika `Code Review`.
Migrated & green: Lint (rust clippy + golangci + ui eslint/prettier), UI build,
unit tests, test-selection, catalog-migrate (pg 14-18), renovate-config check,
the 15-perm flow matrix (+ flow-images / go build & module caches), merge-ready
gate, repo snapshot.

## 1. Parity — to fully replace the GHA workflows
- [ ] **Cloud e2e + nexus cargo test** — keyless cloud auth (OIDC / GCP+Azure
  workload-identity; AWS instance role) to re-enable the 6 disabled cloud perms
  (`postgres_other`×3, `bigquery`×3), **ci.yml's nexus `cargo test`** (the one
  rust job still deferred), and the RDS-IAM / S3-IAM / CloudSQL tests. Snowflake
  may stay a stored secret.
- [ ] **deprecated-connector.yml** — silenced, not yet ported.
- [ ] **flow-api-client / -tag** — maintenance jobs (commit generated protos to
  a branch); not ported.
- [ ] **Renovate-config PR comment** — the check is ported (`Renovate config`
  job), but the GHA github-script step that posts the simulation report on the
  PR is not yet reproduced (output is in the job Result/artifacts for now).
- [ ] **Depot docker builds** (dev/stable/customer/mysql-debug) — currently kept
  on GHA (push-only). Decide: migrate to Praktika (buildx + CI cache) or keep.
- [ ] **Post-merge CI** — Praktika workflow is PR-only; add a push-to-main
  workflow or accept PR-gating only.
- [ ] **Flaky-test retry** — GHA leans on `auto-retry-flaky-tests`; our matrix
  blocks with no retry. Add a retry pass (job-level or port the bot).
- [ ] **Manual repo setting** — require `Ready For Merge [PR]` as a
  branch-protection required check.

Staying on GitHub (intentionally not migrated): CodeQL (workflow kept; PR
trigger commented, runs on push/schedule), the Renovate bot itself,
release-drafter, interactive `@claude`, cleanup, update-*, auto-retry-flaky-tests.

## 2. Improvements — add on top (Praktika features)
- [x] **Cross-model code review** — Praktika job-based review using an OpenAI
  model on Bedrock (`Code Review` job); valuable precisely when the diff was
  written by Claude, since a *different* model reviews it.
- [x] **CI cache / reuse / skip** — digest-keyed jobs (go module cache, go build
  cache, flow images) let Praktika skip the producer and reuse its published
  artifact across runs when the keyed inputs are unchanged. Every other job now
  carries a change-filter digest too (ported from the old GHA `paths:`), so a
  docs/UI/nexus-only PR skips the Go/flow jobs entirely. Note: the consumers'
  digests are what make the producer digests effective — an undigested consumer
  `requires` a producer and would rescue (re-run) it every PR.
- [ ] **Docker pull-through proxy** — cache DB/base image pulls across jobs &
  runs (also dodges Docker Hub limits).
- [ ] **One-host flow run** — instead of a separate job per flow permutation,
  bring the stack up once on one machine and run the permutations one-by-one:
  the tests are fast and per-job Tilt/docker setup dominates, so this should cut
  cost/wall-clock. (Trade-off: less parallelism/isolation.)
- [x] **Dynamic / hook-driven matrix** — a flow-test permutation is skipped when
  the PR's flow changes are confined to *other* setups, via a
  `workflow_filter_hooks` hook (`ci/scripts/flow_filter.py`) keyed on per-setup
  `owns` prefixes in `flow_matrix.py`. Hook skips run before the digest filter
  and are final, so they override the `requires`-rescue without touching the
  shared producer digests. A change to any shared/core flow path (or the stack,
  protos, drivers) still runs every perm; `ci-force-all` bypasses it.
- [ ] **CIDB / test-results ingestion** — reuse Praktika's native CIDB against
  the stresshouse ClickHouse, or keep the existing NDJSON→ClickHouse ingestion
  (`ci_peerdb_test_runs`, what `cidb`/flake tooling queries)? Open question:
  schema / tooling compatibility.
