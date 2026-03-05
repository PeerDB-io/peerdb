# Flow e2e flake analysis (last 20 main commits)

Scope: inspected flow e2e CI runs for the latest 20 commits on `main` (Feb 26–Mar 4). Only two runs failed; both show the same ClickHouse destination-not-found pattern.

## Failing runs
- Run 22661847388 (flow run #16458, commit 4ca4f810 “upgrade docker-compose stable image tags”): `flow_test (mysql-gtid, 6.0, lts)` failed in `TestApiMy/TestQRep`. ClickHouse returned `Unknown table expression identifier 'qrepapi_api_fxd6qyn0'...` during initial load; subsequent wait timed out (`UNEXPECTED TIMEOUT finish`). QRep destination table never appeared.
- Run 22652600730 (flow run #16451, commit bde2cdff “o11y: small refactor on user-facing logs”): `flow_test (maria, 8.0, latest)` failed in `TestBigQueryClickhouseSuite`. Two cases hit the same issue:
  - `Test_Trips_Flow_Small_Partitions`: ClickHouse `Unknown table expression identifier 'trips_1k_dst_small_partitions'...` while comparing row counts.
  - `Test_Types`: timed out waiting for initial load to materialize; destination table stayed empty (`q.NumRecords: 1000`, `other.NumRecords: 0`).

## Pattern
- All failures are “destination table missing/empty in ClickHouse” after the pipeline reports inserts, leading to unknown-table errors or timeouts.
- Hits different suites (API MySQL QRep; BigQuery→ClickHouse) and connectors, suggesting a ClickHouse DDL propagation/race rather than source-specific data.
- Other flow jobs in the same runs passed, so the issue is intermittent rather than systematic.

## Suggested mitigations
- Add explicit waits/retries for ClickHouse table existence before SELECT checks in QRep and BigQuery suites; surface DDL create errors earlier.
- Capture ClickHouse server logs around DDL/initial load to confirm whether tables fail to create or simply lag.
- If reproducible, consider increasing ClickHouse settings for replication delay/DDL logging in CI to reduce eventual-consistency gaps.
