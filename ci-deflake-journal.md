# CI Deflake Journal

## 2026-05-28

- Re-established branch state: `codex-goal-deflake-ci` had only the `init` commit on top of `main`; no prior local journal was present.
- `cidb` result values are `success`, `failure`, and `skipped`, not `PASS`. The initial PR run for this branch was still in progress, so no branch rows were available yet.
- Recent `main` baseline over 14 days: 141 matrix jobs, 33 failed jobs, 23.4% failed job rate. By matrix: pg16/mysql-gtid 11/47 failed, pg17/mysql-pos 12/47 failed, pg18/maria 10/47 failed.
- Failure categories overlap, but job-level signatures included: 12 jobs with `FATAL: terminating connection due to administrator command`, 6 jobs with ClickHouse/Postgres `QValueTime` one-microsecond mismatches, 4 jobs with `runPipeline` broken-pipe source failures, plus setup/status timeouts.
- Experiment 1 patch set:
  - Use `application_name=peerdb_catalog` for catalog pools so tests that intentionally terminate `application_name='peerdb'` source backends do not kill catalog connections in CI, where the catalog DB and source PG are the same service.
  - Replace `now()` values in `Test_Types_CH` with fixed timestamp/time literals to remove the observed 1us time comparison flake.
  - Make the `runPipeline` destination-failure test deterministic and only mark peer process kills as such when `Kill` succeeds.
