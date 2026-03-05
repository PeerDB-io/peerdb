# Flow e2e flake analysis (last 20 commits to main)

Date: 2026-03-05

## Runs inspected
- Run 22661847388 (run_number 16458) on commit 4ca4f810614eab0203f98c3804ee61be1e9bde90, job `flow_test (ubuntu-latest-16-cores, 16, mysql-gtid, 6.0, lts)` — failed.
- Run 22652600730 (run_number 16451) on commit bde2cdff604952c8e2af4fe4c2f3dc5ccadb760a, job `flow_test (ubuntu-latest-16-cores, 18, maria, 8.0, latest)` — failed.
- Earlier main runs within the last 20 commits (e.g., run_number 16200 on commit f76458f2deaeebf898c605d0d94771e26b58232d) succeeded.

## Failure signatures observed
1) ClickHouse destination missing during qrep initial load (MySQL → CH):
   - Test: `e2e TestApiMy/TestQRep`.
   - Error: ClickHouse query errors with `Unknown table expression identifier 'qrepapi_api_fxd6qyn0' ... FINAL`, followed by `UNEXPECTED TIMEOUT` waiting for load (run 22661847388).
   - Symptom: Source row count 2 vs destination 1; eventual timeout while waiting for finish.

2) ClickHouse destination missing during BigQuery → ClickHouse flows:
   - Tests: `TestBigQueryClickhouseSuite/Test_Trips_Flow_Small_Partitions` and `TestBigQueryClickhouseSuite/Test_Types`.
   - Errors: `Unknown table expression identifier 'trips_1k_dst_small_partitions' ... FINAL` and timeout waiting for records to appear (run 22652600730).
   - Symptom: Source rows (e.g., 1000) vs destination 0; repeated checks stuck at 0 until timeout.

## Patterns and hypotheses
- Both failures show ClickHouse complaining that the destination table is absent when the test begins reading with `FINAL`. This looks like a race where the apply-schema or initial-load table creation did not complete (or failed silently) before the verification loop.
- The issue spans different suites (MySQL qrep and BigQuery ingestion) and different CH database names, suggesting a systemic CH table creation / propagation race rather than test data conflict.
- Runs immediately before/after on main have passed, supporting “intermittent flake” rather than deterministic regression.

## Recommended next steps
- Add explicit waits/retries for ClickHouse destination table existence before issuing `SELECT ... FINAL` in flow e2e helpers to mask creation latency.
- Increase logging around ClickHouse apply-schema and initial-load DDL (including failures) to confirm whether DDL is being skipped or delayed.
- If possible, capture and emit ClickHouse DDL execution timing in CI artifacts to correlate with the verification timeouts.
