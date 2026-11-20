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
_PRAKTIKA_VERSION = "0.1.28"
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


def _image_builders():
    image_recipe_version = "1.1.0"
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
    custom_image_tests = [
        Components.create_image_test_component(
            name="project-image-test",
            commands=[
                "test -d /opt/praktika/work",
                "test -w /opt/praktika/work",
                # Toolchain baked by _peerdb_toolchain_component must be on PATH
                # and runnable without any extra env (real toolchain binaries,
                # not rustup shims).
                "command -v protoc",
                "command -v buf",
                "command -v cargo",
                "cargo --version",
                "cargo clippy --version",
            ],
        ),
    ]
    # Build-phase toolchain install runs before the test-phase asserts above.
    custom_components = [_peerdb_toolchain_component(), *custom_image_tests]
    return [
        Components.create_ubuntu_image_builder_config(
            name="ci-arm64-image",
            version=image_recipe_version,
            controller_package=_PRAKTIKA_CONTROLLER_WHL,
            prebuilt_venvs=prebuilt_venvs,
            components=custom_components,
            instance_types=["c7g.large"],
        ),
        Components.create_ubuntu_image_builder_config(
            name="ci-x86_64-image",
            version=image_recipe_version,
            controller_package=_PRAKTIKA_CONTROLLER_WHL,
            prebuilt_venvs=prebuilt_venvs,
            components=custom_components,
            instance_types=["c7a.large"],
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
        min_praktika_version="0.1.28",
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
            instance_type="t4g.small",
            scaling=Components.OrchestratorPool.Scaling.Auto,
            size=0,
            max_size=50,
            volume_size_gb=30,
            capacity_reserve=1,
            image_builder=_IMAGE_BUILDERS_BY_NAME["ci-arm64-image"],
            ext={"allowed_push_branches": ['main'], "allowed_pr_base_branches": ['main'], "system_logs": False, "host_metrics": False, "allowed_users": ["maxknv"]},
        ),
        runner_pools=[
            Components.RunnerPool(
                name="arm-small",
                instance_type="c7g.large",
                scaling=Components.RunnerPool.Scaling.Auto,
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
                # Opt-in CloudWatch observability, off by default (both
                # cost money when on): system_logs = kernel/OOM/systemd
                # evidence; host_metrics = CPU/mem/disk curve for a dead
                # runner. See the RunnerPool docstring.
                ext={"system_logs": False, "host_metrics": False},
            )
        ],
    )
]
