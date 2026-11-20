from praktika import Job, Workflow
from ci.settings.settings import RunnerLabels


WORKFLOWS = [
    Workflow.Config(
        name="PR",
        event=Workflow.Event.PULL_REQUEST,
        base_branches=["main"],

        jobs=[
            # Port of .github/workflows/rust-lint.yml. The Rust toolchain, buf
            # and protoc are baked into the runner image (see
            # ci/infrastructure/projects.py); only the gitignored generated
            # protobuf bindings are produced per run, in the pre-hook.
            Job.Config(
                name="Rust Lint",
                runs_on=[RunnerLabels.ARM_2CPU_4G],
                pre_hooks=["buf generate protos"],
                command="cd nexus && cargo clippy -- -D warnings",
                timeout=60 * 60,
            ),
            # Port of flow.yml's `unit_tests` job (no services). The driver runs
            # gotestsum and emits a structured praktika Result (per-test table,
            # JUnit XML attached) instead of a bare exit-code pass/fail. Protos
            # are generated in the pre-hook; the Go toolchain + gotestsum are
            # baked into the x86_64 image (ci/infrastructure/projects.py).
            Job.Config(
                name="Unit tests (no services)",
                runs_on=[RunnerLabels.AMD_64CPU_128G],
                pre_hooks=["./generate-protos.sh"],
                command="python3 ci/scripts/unit_tests.py",
                timeout=30 * 60,
            ),
            # Port of flow.yml's `test_selection_coverage` job.
            Job.Config(
                name="Test selection coverage",
                runs_on=[RunnerLabels.AMD_64CPU_128G],
                pre_hooks=["./generate-protos.sh"],
                command="bash .github/scripts/check-flow-test-selection.sh",
                timeout=20 * 60,
            ),
            # First Tilt e2e permutation (port of flow.yml's flow_test matrix,
            # one target: postgres-16 + ClickHouse lts). The driver
            # ci/scripts/flow_test.py brings up the docker-compose stack via Tilt
            # and runs gotestsum on the host, emitting a structured praktika
            # Result (per-test table, diagnostics attached). allow_failure for
            # now: the AWS/RDS-IAM/CloudSQL/cloud-destination suites are skipped
            # until their secrets are re-homed (Phase B), and we want the run
            # visible without blocking merge while we shake the plumbing out.
            Job.Config(
                name="Flow test (postgres-16_ch-lts)",
                runs_on=[RunnerLabels.AMD_64CPU_128G],
                command=(
                    'export RUN_LABEL="postgres-16_ch-lts" && '
                    'export TEST_PACKAGES="'
                    "./e2e/postgres_clickhouse/... ./e2e/postgres_postgres/... "
                    "./e2e/switchboard_postgres/... ./alerting/... "
                    "./connectors/postgres/... ./connectors/clickhouse/... "
                    "./connectors/utils/... ./connectors/external_metadata/... ./db/..."
                    '" && '
                    'export PKG_TEST_PACKAGES="./clickhouse/..." && '
                    'export ANCILLARY_SERVICES="otel-collector toxiproxy openssh '
                    'clickhouse clickhouse-keeper clickhouse-02 postgres postgres2" && '
                    'export POSTGRES_IMAGE="imresamu/postgis:16-3.5-alpine" && '
                    'export CLICKHOUSE_IMAGE="clickhouse/clickhouse-server:26.8" && '
                    "python3 ci/scripts/flow_test.py"
                ),
                timeout=60 * 60,
                allow_failure=True,
            ),
        ],
        enable_cache=True,
        enable_report=True,
        enable_gh_summary_comment=True,
        enable_exit_code_result=True,
    )
]
