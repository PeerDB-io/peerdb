from praktika.infrastructure import (
    CloudInfrastructure,
    Components,
    ImageBuilder,
    Storage,
    VPC,
)
from praktika.settings import Settings


# S3 prefixes the runner instance roles are scoped to. Both the artifact
# and the report bucket are granted (they are often the same bucket, in
# which case the duplicate collapses away).
_PROJECT_S3_PREFIXES = list(
    dict.fromkeys(
        [
            f"{Settings.S3_ARTIFACT_BUCKET}/*",
            f"{Settings.S3_REPORT_BUCKET}/*",
        ]
    )
)


# until published in pip
_PRAKTIKA_PACKAGE_BASE_URL = (
    "https://praktika-artifacts-eu-north-1.s3.amazonaws.com/packages"
)
# praktika and praktika-controller are versioned in lockstep (single
# source of truth), so one pinned version drives both wheels.
_PRAKTIKA_VERSION = "0.1.32"
_PRAKTIKA_WHL = f"{_PRAKTIKA_PACKAGE_BASE_URL}/praktika-{_PRAKTIKA_VERSION}-py3-none-any.whl"
_PRAKTIKA_CONTROLLER_WHL = (
    f"{_PRAKTIKA_PACKAGE_BASE_URL}/praktika_controller-{_PRAKTIKA_VERSION}-py3-none-any.whl"
)


# Pins for the toolchain baked into the runner image.
#   Rust is pinned to the same version the production binary is built with
#   (see stacks/peerdb-server.Dockerfile). protoc matches
#   scripts/install-protobuf.sh. buf tracks "latest", mirroring the
#   bufbuild/buf-action default used in the GitHub Actions workflows.
_RUST_VERSION = "1.99.1"
_PROTOC_VERSION = "23.4"

# Pins for the flow e2e host toolchain (baked into the x86_64 image used by the
# heavy Tilt pool). Go matches .github/workflows/flow.yml; Tilt matches the pin
# in .github/workflows/tilt-flow.yml.
_GO_VERSION = "1.27.4"
_TILT_VERSION = "0.37.0"


def _peerdb_toolchain_component():
    """Build-phase image component: everything the Rust lint/build jobs need
    baked into the AMI so they don't reinstall per run.

    Installs the nexus build deps (pkg-config/libssl-dev, matching
    stacks/peerdb-server.Dockerfile), protoc, buf, and a pinned Rust toolchain
    with clippy. The real toolchain binaries (not the rustup shims) are
    symlinked onto PATH so jobs work without exporting RUSTUP_HOME.

    AWS Image Builder runs each list entry as its own shell, so anything that
    shares a shell variable or `export` is chained within a single entry.
    """
    return {
        "name": "peerdb-toolchain",
        "platform": "Linux",
        "phase": "build",
        "version": "1.0.0",
        "description": "Rust + buf + protoc + nexus build deps",
        "commands": [
            # build-essential/curl/unzip/jq/git come from the base setup component
            "DEBIAN_FRONTEND=noninteractive apt-get update && "
            "DEBIAN_FRONTEND=noninteractive apt-get install --yes "
            "--no-install-recommends pkg-config libssl-dev",
            # protoc (version-matched to scripts/install-protobuf.sh)
            "set -eux; ARCH=$(uname -m); "
            'case "$ARCH" in arm64|aarch64) ARCH=aarch_64 ;; esac; '
            "curl -fsSL -o /tmp/protoc.zip "
            '"https://github.com/protocolbuffers/protobuf/releases/download/'
            f'v{_PROTOC_VERSION}/protoc-{_PROTOC_VERSION}-linux-$ARCH.zip"; '
            'unzip -o /tmp/protoc.zip -d /usr/local bin/protoc "include/*"; '
            "rm -f /tmp/protoc.zip; protoc --version",
            # buf (latest release, matching bufbuild/buf-action)
            "set -eux; "
            "BUF_VERSION=$(curl -fsSL "
            "https://api.github.com/repos/bufbuild/buf/releases/latest "
            "| jq -r .tag_name); "
            "curl -fsSL -o /usr/local/bin/buf "
            '"https://github.com/bufbuild/buf/releases/download/'
            '${BUF_VERSION}/buf-$(uname -s)-$(uname -m)"; '
            "chmod +x /usr/local/bin/buf; buf --version",
            # Rust toolchain under /opt/rust; symlink the real (non-shim)
            # toolchain binaries so jobs need no RUSTUP_HOME/CARGO_HOME.
            "set -eux; export RUSTUP_HOME=/opt/rust/rustup "
            "CARGO_HOME=/opt/rust/cargo; "
            "curl -fsSL https://sh.rustup.rs | sh -s -- -y --no-modify-path "
            f"--profile minimal --default-toolchain {_RUST_VERSION} "
            "--component clippy; "
            "ln -sf /opt/rust/rustup/toolchains/*/bin/* /usr/local/bin/; "
            "cargo --version; cargo clippy --version",
        ],
    }


def _peerdb_flow_host_toolchain_component():
    """Build-phase component for the flow e2e jobs (heavy x86_64 pool only).

    Bakes the host-side toolchain .github/workflows/tilt-flow.yml installs per
    run: Go, gotestsum, Tilt, libgeos-dev, the v18 postgres client (pg_dump),
    the `retry` CLI (used by test-results ingestion), zstd and yq. Docker, jq,
    buf and the Rust toolchain come from earlier components. The DB clients the
    Tilt provision scripts use run inside the service containers via
    `docker exec`, so they are deliberately not installed here.

    Each list entry is its own shell (AWS Image Builder), so multi-step
    sequences that share state are chained within a single entry.
    """
    return {
        "name": "peerdb-flow-host-toolchain",
        "platform": "Linux",
        "phase": "build",
        "version": "1.0.4",
        "description": "Go + gotestsum + Tilt + docker-compose + flow e2e host deps",
        "commands": [
            # Give containers a high default nofile, matching GitHub-hosted
            # runners. The compose files set no per-service ulimit, so a service
            # like mongod inherits the daemon default; our base image leaves that
            # at 1024, so mongod hits "Too many open files" under CI load and
            # crash-loops. Merge default-ulimits into the baked daemon.json
            # (dockerd reads it at runner boot).
            "set -eux; "
            'jq \'. + {"default-ulimits": {"nofile": {"Name":"nofile","Hard":1048576,"Soft":1048576}}}\' '
            "/etc/docker/daemon.json > /tmp/daemon.json && mv /tmp/daemon.json /etc/docker/daemon.json; "
            "jq -e '.\"default-ulimits\".nofile.Soft == 1048576' /etc/docker/daemon.json",
            # docker-compose standalone binary: Tilt drives the stack by exec'ing
            # `docker-compose` (not the `docker compose` plugin), and the base
            # image installs neither. Install the v2 standalone as docker-compose.
            "set -eux; ARCH=$(uname -m); "
            "DC_VERSION=$(curl -fsSL "
            "https://api.github.com/repos/docker/compose/releases/latest | jq -r .tag_name); "
            'curl -fsSL -o /usr/local/bin/docker-compose '
            '"https://github.com/docker/compose/releases/download/${DC_VERSION}/docker-compose-linux-$ARCH"; '
            "chmod +x /usr/local/bin/docker-compose; docker-compose version",
            # golangci-lint (pinned to .github/workflows/golang-lint.yml's v2.15.0).
            "set -eux; ARCH=$(dpkg --print-architecture); V=2.15.0; "
            'curl -fsSL "https://github.com/golangci/golangci-lint/releases/download/'
            'v${V}/golangci-lint-${V}-linux-$ARCH.tar.gz" -o /tmp/gcl.tgz; '
            "tar -xzf /tmp/gcl.tgz -C /tmp; "
            "mv /tmp/golangci-lint-${V}-linux-$ARCH/golangci-lint /usr/local/bin/; "
            "rm -rf /tmp/gcl.tgz /tmp/golangci-lint-${V}-linux-$ARCH; golangci-lint version",
            # Node.js LTS (ui lint/build; GHA uses setup-node's default LTS).
            "set -eux; NODE_VERSION=22.11.0; ARCH=$(dpkg --print-architecture); "
            'case "$ARCH" in amd64) NARCH=x64 ;; arm64) NARCH=arm64 ;; esac; '
            'curl -fsSL "https://nodejs.org/dist/v${NODE_VERSION}/node-v${NODE_VERSION}-linux-${NARCH}.tar.gz" -o /tmp/node.tgz; '
            "tar -xzf /tmp/node.tgz -C /opt; rm -f /tmp/node.tgz; "
            "ln -sf /opt/node-v${NODE_VERSION}-linux-${NARCH}/bin/node "
            "/opt/node-v${NODE_VERSION}-linux-${NARCH}/bin/npm "
            "/opt/node-v${NODE_VERSION}-linux-${NARCH}/bin/npx /usr/local/bin/; "
            "node --version; npm --version",
            # apt: geos, retry, postgresql-client-18 (via PGDG), zstd.
            "set -eux; export DEBIAN_FRONTEND=noninteractive; "
            "install -d /usr/share/postgresql-common/pgdg; "
            "curl -fsSL -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc "
            "https://www.postgresql.org/media/keys/ACCC4CF8.asc; "
            'echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] '
            'https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" '
            "> /etc/apt/sources.list.d/pgdg.list; "
            "apt-get update; "
            "apt-get install --yes --no-install-recommends "
            "libgeos-dev retry postgresql-client-18 zstd; "
            # pg_dump >= server major; put the v18 client binaries on PATH.
            "ln -sf /usr/lib/postgresql/18/bin/* /usr/local/bin/; "
            "pg_dump --version; retry --help >/dev/null",
            # Go (arch-aware: dpkg arch == go.dev arch naming).
            "set -eux; ARCH=$(dpkg --print-architecture); "
            f'curl -fsSL "https://go.dev/dl/go{_GO_VERSION}.linux-$ARCH.tar.gz" -o /tmp/go.tgz; '
            "rm -rf /usr/local/go; tar -C /usr/local -xzf /tmp/go.tgz; rm -f /tmp/go.tgz; "
            "ln -sf /usr/local/go/bin/go /usr/local/go/bin/gofmt /usr/local/bin/; "
            "go version",
            # gotestsum (installed system-wide via GOBIN). Image Builder runs
            # components as root with no HOME, so Go can't locate a build cache;
            # set HOME/GOCACHE explicitly.
            "set -eux; export HOME=/root GOCACHE=/root/.cache/go-build "
            "GOBIN=/usr/local/bin GOPATH=/root/go; "
            "/usr/local/bin/go install gotest.tools/gotestsum@latest; "
            "gotestsum --version",
            # Tilt (install.sh honours TILT_VERSION; lands in /usr/local/bin as
            # root). HOME is set so the installer never falls back to an unset
            # $HOME/.local/bin.
            "set -eux; export HOME=/root; curl -fsSL "
            "https://raw.githubusercontent.com/tilt-dev/tilt/master/scripts/install.sh "
            f"| TILT_VERSION={_TILT_VERSION} bash; "
            "command -v tilt || cp /root/.local/bin/tilt /usr/local/bin/tilt; "
            "tilt version",
            # yq (flow Tiltfile / generate-test-environment.sh read the matrix).
            "set -eux; ARCH=$(dpkg --print-architecture); "
            "YQ_VERSION=$(curl -fsSL "
            "https://api.github.com/repos/mikefarah/yq/releases/latest | jq -r .tag_name); "
            'curl -fsSL -o /usr/local/bin/yq '
            '"https://github.com/mikefarah/yq/releases/download/${YQ_VERSION}/yq_linux_$ARCH"; '
            "chmod +x /usr/local/bin/yq; yq --version",
        ],
    }


def _image_builders():
    image_recipe_version = "1.1.13"
    prebuilt_venvs = [
        # The `infrastructure` extra pulls Praktika's runtime deps
        # (boto3/PyJWT/cryptography/requests) automatically; pytest is
        # an optional extra the runner needs, so list it explicitly.
        ImageBuilder.PrebuiltVenv(
            name=Settings.PRAKTIKA_BASE_VENV,
            packages=[
                "pytest>=7.0.0",
                "pytest-reportlog>=0.4.0",
                f"praktika[infrastructure] @ {_PRAKTIKA_WHL}",
            ],
            description="CI runtime venv",
        ),
    ]
    # Shared asserts: the Rust toolchain must be on PATH and runnable with no
    # extra env (real toolchain binaries, not rustup shims).
    _rust_asserts = [
        "test -d /opt/praktika/work",
        "test -w /opt/praktika/work",
        "command -v protoc",
        "command -v buf",
        "command -v cargo",
        "cargo --version",
        "cargo clippy --version",
    ]
    arm_image_test = Components.create_image_test_component(
        name="project-image-test",
        commands=_rust_asserts,
    )
    # x86_64 also carries the flow e2e host toolchain.
    x86_image_test = Components.create_image_test_component(
        name="project-image-test",
        commands=[
            *_rust_asserts,
            "command -v go",
            "go version",
            "command -v gotestsum",
            "command -v tilt",
            "command -v docker-compose",
            "docker-compose version",
            "command -v golangci-lint",
            "command -v node",
            "command -v npm",
            # Containers must default to a high nofile (see the daemon.json merge).
            "jq -e '.\"default-ulimits\".nofile.Soft == 1048576' /etc/docker/daemon.json",
            "command -v yq",
            "command -v pg_dump",
            "command -v retry",
            "command -v zstd",
            "command -v docker",
        ],
    )
    rust_toolchain = _peerdb_toolchain_component()
    flow_toolchain = _peerdb_flow_host_toolchain_component()
    return [
        Components.create_ubuntu_image_builder_config(
            name="ci-arm64-image",
            version=image_recipe_version,
            controller_package=_PRAKTIKA_CONTROLLER_WHL,
            prebuilt_venvs=prebuilt_venvs,
            # Build-phase install runs before the test-phase asserts.
            components=[rust_toolchain, arm_image_test],
            instance_types=["c8g.large"],
        ),
        Components.create_ubuntu_image_builder_config(
            name="ci-x86_64-image",
            version=image_recipe_version,
            controller_package=_PRAKTIKA_CONTROLLER_WHL,
            prebuilt_venvs=prebuilt_venvs,
            components=[rust_toolchain, flow_toolchain, x86_image_test],
            instance_types=["c8i.large"],
        ),
    ]


_GH_TOKEN_MINTER = Components.GitHubTokenMinter(
    permissions={
        "checks": "write",
        "contents": "write",
        "issues": "write",
        "metadata": "read",
        "pages": "write",
        "pull_requests": "write",
        "statuses": "write",
    },
    repositories=[Settings.PROJECT_NAME],
)
_IMAGE_BUILDERS = _image_builders()
_IMAGE_BUILDERS_BY_NAME = {builder.name: builder for builder in _IMAGE_BUILDERS}


PROJECTS = [
    CloudInfrastructure.Config(
        name=Settings.PROJECT_NAME,
        min_praktika_version="0.1.32",
        vpcs=[
            VPC.Config(
                subnets=[
                    VPC.Subnet(availability_zone="eu-west-1a"),
                ],
            )
        ],
        storages=[
            Storage.Config(
                name="artifacts-eu-west-1",
                retention_days=30,
                public=True,
            ),
        ],
        report_pages=[Components.report_page_config],
        image_builders=_IMAGE_BUILDERS,
        github_token_minters=[_GH_TOKEN_MINTER],
        # Warm clone (optional, SSM-tunable): set ci_config["repo"] =
        # "owner/name" and ci_config["warm_branches"] = ["main"]
        # so an idle reserved orchestrator pre-fetches those branches and the
        # per-task clone only applies the PR delta (no full head fetch, no
        # unshallow). See praktika/docs/ci-config.md.
        orchestrator_pool=Components.OrchestratorPool(
            instance_type="t4g.medium",
            scaling=Components.OrchestratorPool.Scaling.Auto,
            size=0,
            max_size=50,
            volume_size_gb=30,
            capacity_reserve=2,
            scale_coefficient=1,
            image_builder=_IMAGE_BUILDERS_BY_NAME["ci-arm64-image"],
            ext={"allowed_push_branches": ['main'], "allowed_pr_base_branches": ['main'], "system_logs": False, "host_metrics": False, "allowed_users": ["maxknv"]},
        ),
        runner_pools=[
            Components.RunnerPool(
                name="arm_2cpu_4g",
                instance_type="c8g.large",
                scaling=Components.RunnerPool.Scaling.Auto,
                scale_coefficient=1,
                size=0,
                max_size=50,
                volume_size_gb=60,
                image_builder=_IMAGE_BUILDERS_BY_NAME["ci-arm64-image"],
                allowed_ssm_parameters=[],
                allowed_secrets=[],
                allowed_s3_prefixes=_PROJECT_S3_PREFIXES,
                allow_all_ssm_parameters=False,
                allow_all_secrets=False,
                allow_all_s3_prefixes=False,
                allow_ssm_debug=False,
                capacity_reserve=1,
                # Opt-in CloudWatch observability, off by default (both
                # cost money when on): system_logs = kernel/OOM/systemd
                # evidence; host_metrics = CPU/mem/disk curve for a dead
                # runner. See the RunnerPool docstring.
                # Bedrock grant: this (otherwise idle, post rust-lint-merge) arm
                # pool runs the Praktika AI Code Review job, which invokes a
                # Bedrock model via the Converse API.
                ext={
                    "system_logs": False,
                    "host_metrics": False,
                    "iam_statements": [
                        {
                            "Sid": "BedrockRuntimeInference",
                            "Effect": "Allow",
                            "Action": ["bedrock:InvokeModel"],
                            "Resource": [
                                "arn:aws:bedrock:*::foundation-model/*",
                                "arn:aws:bedrock:*:*:inference-profile/*",
                            ],
                        }
                    ],
                },
            ),
            # Heavy x86_64 pool for the Tilt e2e stack (full docker-compose stack
            # + highly parallel go test). Matches GitHub's ubuntu-latest-32-cores
            # on CPU; 1:2 vCPU:RAM (32 vCPU / 64 GiB) since RAM isn't scarce in
            # CI. Latest-gen Intel (c8i). Runs the Tilt e2e matrix + unit tests.
            Components.RunnerPool(
                name="amd_32cpu_64g",
                instance_type="c8i.8xlarge",
                scaling=Components.RunnerPool.Scaling.Auto,
                scale_coefficient=1,
                size=0,
                max_size=20,
                volume_size_gb=200,
                # The Tilt e2e stack is IO-bound (minio S3 staging, CH/PG/mongo
                # data dirs, docker image load, Go cache) under -parallel 64; the
                # gp3 baseline (3000 IOPS / 125 MiB/s) starves it vs GitHub's
                # local-NVMe runners. Provision gp3 to its max.
                volume_iops=16000,
                volume_throughput=1000,
                image_builder=_IMAGE_BUILDERS_BY_NAME["ci-x86_64-image"],
                allowed_ssm_parameters=[],
                allowed_secrets=[],
                allowed_s3_prefixes=_PROJECT_S3_PREFIXES,
                allow_all_ssm_parameters=False,
                allow_all_secrets=False,
                allow_all_s3_prefixes=False,
                allow_ssm_debug=False,
                capacity_reserve=3,
                ext={"system_logs": False, "host_metrics": False},
            ),
            # Smaller x86 pool for jobs that can't saturate the big one: the
            # flow-image docker build (serial Go link) and the light test-
            # selection check. Latest-gen Intel (c8i).
            Components.RunnerPool(
                name="amd_8cpu_16g",
                instance_type="c8i.2xlarge",
                scaling=Components.RunnerPool.Scaling.Auto,
                scale_coefficient=1,
                size=0,
                max_size=20,
                volume_size_gb=120,
                # Build throughput helps (docker build + go mod download); no need
                # for the e2e pool's max IOPS.
                volume_iops=16000,
                volume_throughput=1000,
                image_builder=_IMAGE_BUILDERS_BY_NAME["ci-x86_64-image"],
                allowed_ssm_parameters=[],
                allowed_secrets=[],
                allowed_s3_prefixes=_PROJECT_S3_PREFIXES,
                allow_all_ssm_parameters=False,
                allow_all_secrets=False,
                allow_all_s3_prefixes=False,
                allow_ssm_debug=False,
                # Keep instances warm: this pool carries the dependency-free
                # "first" jobs (Flow images, UI build, Renovate config) that
                # dispatch the instant the workflow starts. Without a reserve each
                # eats an EC2 cold-start at the front of the DAG. Matches the
                # first-job fan-out, mirroring arm_2cpu_4g's reserve=1 for Code
                # Review.
                capacity_reserve=0,
                ext={"system_logs": False, "host_metrics": False},
            ),
        ],
    )
]
