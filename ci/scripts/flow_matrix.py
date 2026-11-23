#!/usr/bin/env python3
"""Data-driven port of flow.yml's `flow_test` matrix.

flow.yml fans out 21 permutations from three tables (matrix.target,
matrix.jobs, matrix.images) plus the `with:` expression block that maps each
target to the reusable tilt-flow workflow's inputs. This module is the single
source of truth for that same data, consumed by:

  - ci/scripts/flow_test.py  -> resolve(MATRIX_TARGET) for the per-run env
  - ci/workflows/pull_request.py -> one parametrized Job per target label

Keep this in sync with .github/workflows/flow.yml until that file is retired.
"""

# Source job -> packages / pkg / base ancillary services (flow.yml matrix.jobs).
#
# `owns`: repo-relative path prefixes that are EXCLUSIVE to this setup -- its
# e2e test dirs, source connector, and source pkg. Used by the flow-test filter
# hook (ci/scripts/flow_filter.py) to skip a permutation when a PR's flow
# changes are confined to *other* setups. Deliberately omits shared code (the
# clickhouse destination connector/pkg, connectors/utils, connectors/core.go,
# db, alerting, activities, workflows, model, ...): a change to any of those is
# NOT owned by a setup, so the hook treats it as core and runs every perm. Keep
# `owns` conservative -- an over-broad entry risks skipping a perm that a change
# actually affects.
JOBS = {
    "postgres_clickhouse": {
        "packages": [
            "./e2e/postgres_clickhouse/...",
            "./e2e/postgres_postgres/...",
            "./e2e/switchboard_postgres/...",
            "./alerting/...",
            "./connectors/postgres/...",
            "./connectors/clickhouse/...",
            "./connectors/utils/...",
            "./connectors/external_metadata/...",
            "./db/...",
        ],
        "pkg": "./clickhouse/...",
        "services": "otel-collector toxiproxy openssh clickhouse clickhouse-keeper clickhouse-02 postgres postgres2",
        "owns": [
            "flow/e2e/postgres_clickhouse",
            "flow/e2e/postgres_postgres",
            "flow/e2e/switchboard_postgres",
            "flow/connectors/postgres",
        ],
    },
    "postgres_other": {
        "packages": ["./e2e/postgres_other/..."],
        "pkg": "",
        "services": "otel-collector toxiproxy openssh postgres",
        "owns": ["flow/e2e/postgres_other", "flow/connectors/postgres"],
    },
    "mysql_clickhouse": {
        "packages": [
            "./e2e/mysql_clickhouse/...",
            "./e2e/switchboard_mysql/...",
            "./connectors/mysql/...",
        ],
        "pkg": "./mysql/...",
        "services": "otel-collector toxiproxy openssh clickhouse clickhouse-keeper clickhouse-02",
        "owns": [
            "flow/e2e/mysql_clickhouse",
            "flow/e2e/switchboard_mysql",
            "flow/connectors/mysql",
            "flow/pkg/mysql",
        ],
    },
    "mongo_clickhouse": {
        "packages": [
            "./e2e/mongo_clickhouse/...",
            "./e2e/switchboard_mongo/...",
            "./connectors/mongo/...",
        ],
        "pkg": "./mongo/...",
        "services": "otel-collector toxiproxy openssh clickhouse clickhouse-keeper clickhouse-02 mongodb",
        "owns": [
            "flow/e2e/mongo_clickhouse",
            "flow/e2e/switchboard_mongo",
            "flow/connectors/mongo",
            "flow/pkg/mongo",
        ],
    },
    "cockroachdb_clickhouse": {
        "packages": ["./e2e/cockroachdb_clickhouse/..."],
        "pkg": "",
        "services": "otel-collector toxiproxy openssh clickhouse clickhouse-keeper clickhouse-02 cockroachdb",
        # CockroachDB speaks the pg wire protocol, so it rides the postgres
        # connector -- a postgres-connector change runs cockroach perms too.
        "owns": ["flow/e2e/cockroachdb_clickhouse", "flow/connectors/postgres"],
    },
    "bigquery_clickhouse": {
        "packages": ["./e2e/bigquery_clickhouse/...", "./connectors/bigquery/..."],
        "pkg": "",
        "services": "otel-collector toxiproxy openssh clickhouse clickhouse-keeper clickhouse-02",
        "owns": ["flow/e2e/bigquery_clickhouse", "flow/connectors/bigquery"],
    },
}

# Flavor -> docker image (flow.yml matrix.images).
IMAGES = {
    "mysql": {"mysql-gtid": "mysql:9.5", "mysql-pos": "mysql:5.7"},
    "mariadb": {
        "maria-11": "mariadb:lts-ubi9",
        "maria-12": "mariadb:12-ubi",
        "maria-13": "mariadb:13.0-ubi-rc",
    },
    "clickhouse": {
        "lts": "clickhouse/clickhouse-server:26.8",
        "stable": "clickhouse/clickhouse-server:26.9",
        "latest": "clickhouse/clickhouse-server:latest",
    },
    "cockroachdb": {
        "v24": "cockroachdb/cockroach:v24.1.30",
        "v25": "cockroachdb/cockroach:v25.4.14",
        "v26": "cockroachdb/cockroach:v26.2.5",
    },
}

# The 21 permutations (flow.yml matrix.target): (job, version, ch, label).
# `version` is "" where the source job has none (bigquery).
TARGETS = [
    # All 21 flow.yml permutations are represented (so the test-selection check
    # sees every source job); `enabled: False` ones are defined but not run.
    # The cloud-destination perms (postgres_other, bigquery) are disabled until
    # keyless cloud auth lands (ci/TODO.md) — they only fail with
    # "TEST_BQ_CREDS env var not set" today. LABELS (the Praktika matrix) is the
    # enabled subset.
    {"job": "postgres_clickhouse", "version": "16", "ch": "lts", "label": "postgres-16_ch-lts"},
    {"job": "postgres_clickhouse", "version": "17", "ch": "stable", "label": "postgres-17_ch-stable"},
    {"job": "postgres_clickhouse", "version": "18", "ch": "latest", "label": "postgres-18_ch-latest"},
    {"job": "postgres_other", "version": "16", "ch": "", "label": "postgres-16_other", "enabled": False},
    {"job": "postgres_other", "version": "17", "ch": "", "label": "postgres-17_other", "enabled": False},
    {"job": "postgres_other", "version": "18", "ch": "", "label": "postgres-18_other", "enabled": False},
    {"job": "mysql_clickhouse", "version": "mysql-gtid", "ch": "lts", "label": "mysql-gtid_ch-lts"},
    {"job": "mysql_clickhouse", "version": "mysql-pos", "ch": "stable", "label": "mysql-pos_ch-stable"},
    {"job": "mysql_clickhouse", "version": "mysql-gtid", "ch": "latest", "label": "mysql-gtid_ch-latest"},
    {"job": "mysql_clickhouse", "version": "maria-11", "ch": "lts", "label": "maria-11_ch-lts"},
    {"job": "mysql_clickhouse", "version": "maria-12", "ch": "stable", "label": "maria-12_ch-stable"},
    {"job": "mysql_clickhouse", "version": "maria-13", "ch": "latest", "label": "maria-13_ch-latest"},
    {"job": "mongo_clickhouse", "version": "6.0", "ch": "lts", "label": "mongo-6_ch-lts"},
    {"job": "mongo_clickhouse", "version": "7.0", "ch": "stable", "label": "mongo-7_ch-stable"},
    {"job": "mongo_clickhouse", "version": "8.2.12", "ch": "latest", "label": "mongo-8_ch-latest"},
    {"job": "cockroachdb_clickhouse", "version": "v24", "ch": "lts", "label": "cockroachdb-v24_ch-lts"},
    {"job": "cockroachdb_clickhouse", "version": "v25", "ch": "stable", "label": "cockroachdb-v25_ch-stable"},
    {"job": "cockroachdb_clickhouse", "version": "v26", "ch": "latest", "label": "cockroachdb-v26_ch-latest"},
    {"job": "bigquery_clickhouse", "version": "", "ch": "lts", "label": "bigquery_ch-lts", "enabled": False},
    {"job": "bigquery_clickhouse", "version": "", "ch": "stable", "label": "bigquery_ch-stable", "enabled": False},
    {"job": "bigquery_clickhouse", "version": "", "ch": "latest", "label": "bigquery_ch-latest", "enabled": False},
]

# The Praktika matrix runs only enabled targets; check-flow-test-selection sees
# all of them (every source job must stay covered).
LABELS = [t["label"] for t in TARGETS if t.get("enabled", True)]
_BY_LABEL = {t["label"]: t for t in TARGETS}


def owned_prefixes(label):
    """Path prefixes exclusive to `label`'s setup (its JOBS[...]['owns'])."""
    return JOBS[_BY_LABEL[label]["job"]].get("owns", [])


# Union of every setup's owned prefixes; a changed flow path outside this set is
# shared/core and must run every permutation (see ci/scripts/flow_filter.py).
ALL_OWNED = sorted({p for cfg in JOBS.values() for p in cfg.get("owns", [])})


def resolve(label):
    """Resolve a target label to the env a flow_test run needs, mirroring
    flow.yml's `with:` block. Returns a dict of string env vars."""
    if label not in _BY_LABEL:
        raise KeyError(f"unknown flow matrix target: {label!r}")
    t = _BY_LABEL[label]
    job, version, ch = t["job"], t["version"], t["ch"]
    base = JOBS[job]

    services = base["services"].split()
    images = {}

    if job.startswith("postgres_"):
        images["POSTGRES_IMAGE"] = f"imresamu/postgis:{version}-3.5-alpine"
    if job == "mysql_clickhouse":
        if version in ("mysql-gtid", "mysql-pos"):
            services.append(version)  # the service name matches the flavor
            key = "MYSQL_GTID_IMAGE" if version == "mysql-gtid" else "MYSQL_POS_IMAGE"
            images[key] = IMAGES["mysql"][version]
        elif version.startswith("maria-"):
            services.append("mariadb")
            images["MARIADB_IMAGE"] = IMAGES["mariadb"][version]
    if job == "mongo_clickhouse":
        images["MONGODB_IMAGE"] = f"mongo:{version}"
    if job == "cockroachdb_clickhouse":
        images["COCKROACHDB_IMAGE"] = IMAGES["cockroachdb"][version]
    if ch:
        images["CLICKHOUSE_IMAGE"] = IMAGES["clickhouse"][ch]

    # Per-run test env (flow.yml test_env). FLOW_TESTS_RDS_IAM_AUTH_SKIP is left
    # to flow_test.py's default (true) until the cloud secrets are re-homed; at
    # that point set it to "false" here to match flow.yml.
    ci_mysql_version = version if job == "mysql_clickhouse" else ""
    if version.startswith("maria-"):
        ci_mysql_port = "3308"
    elif version == "mysql-pos":
        ci_mysql_port = "3307"
    else:
        ci_mysql_port = "3306"
    test_env = f"CI_MYSQL_VERSION={ci_mysql_version}\nCI_MYSQL_PORT={ci_mysql_port}"

    return {
        "RUN_LABEL": label,
        "TEST_PACKAGES": " ".join(base["packages"]),
        "PKG_TEST_PACKAGES": base["pkg"],
        "ANCILLARY_SERVICES": " ".join(services),
        "TEST_ENV": test_env,
        **images,
    }


def export():
    """The matrix as plain JSON for the shell scripts (check-flow-test-selection.sh,
    generate-test-environment.sh) — the single source of truth that replaces their
    old `yq` reads of .github/workflows/flow.yml. `packages`/`pkg` are joined into
    space-separated strings to match the old flow.yml shape."""
    return {
        "jobs": {
            name: {"packages": " ".join(cfg["packages"]), "pkg": cfg["pkg"]}
            for name, cfg in JOBS.items()
        },
        "targets": TARGETS,
        "images": IMAGES,
    }


if __name__ == "__main__":
    import json

    print(json.dumps(export()))
