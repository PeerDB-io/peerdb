import os
import sys

from praktika import Artifact, Job, Workflow
from ci.settings.settings import RunnerLabels

# flow_matrix lives under ci/scripts (the job scripts dir), not a package.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
from flow_matrix import LABELS  # noqa: E402

# Flow images (flow-api/worker/snapshot-worker) built once and shared with the
# matrix as a docker-save+zstd tarball, so jobs load them instead of each
# rebuilding (port of flow.yml's flow_images job). See ci/scripts/flow_images.py
# and the loader in ci/scripts/flow_test.py.
FLOW_IMAGES_ARTIFACT = Artifact.Config(
    name="flow-images",
    type=Artifact.Type.S3,
    path="flow-images.tar.zst",
)

# Go build cache: every flow test package compiled once and shared with the
# matrix as a GOCACHE tarball, so the permutations restore it instead of each
# recompiling (port of flow.yml's go_build_cache job). See
# ci/scripts/go_build_cache.py and the loader in ci/scripts/flow_test.py.
GO_BUILD_CACHE_ARTIFACT = Artifact.Config(
    name="go-build-cache",
    type=Artifact.Type.S3,
    path="go-build-cache.tar.zst",
)

# Go module cache: `go mod download` once, shared with every Go job as a
# GOMODCACHE tarball (ci/scripts/go_mod_cache.py). Restored via
# `ci/scripts/go_cache.py restore mod`. Unlike the build cache this rarely
# changes, so the producer job is digest-cached on go.mod/go.sum below and
# Praktika reuses the artifact across runs until dependencies move -- the
# persisted, go.sum-keyed module cache GHA's setup-go gave us.
GO_MOD_CACHE_ARTIFACT = Artifact.Config(
    name="go-mod-cache",
    type=Artifact.Type.S3,
    path="go-mod-cache.tar.zst",
)

# Rebuild the module cache only when the dependency set or the producer logic
# changes; otherwise Praktika's job cache reuses the published artifact.
_GO_MOD_CACHE_DIGEST = Job.CacheDigestConfig(
    include_paths=[
        "./flow/go.mod",
        "./flow/go.sum",
        "./flow/pkg/go.mod",
        "./flow/pkg/go.sum",
        "./ci/scripts/go_mod_cache.py",
        "./ci/scripts/go_cache.py",
    ],
)

# Consumer pre-hook: restore the shared module cache before any Go work.
_RESTORE_GO_MOD = "python3 ci/scripts/go_cache.py restore mod"


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
            # No Go here (nexus is Rust), so it needs no module cache.
            Job.Config(
                name="Rust Lint",
                runs_on=[RunnerLabels.ARM_2CPU_4G],
                pre_hooks=["buf generate protos"],
                command="cd nexus && cargo clippy -- -D warnings",
                timeout=60 * 60,
            ),
            # `go mod download` once, published as the go-mod-cache artifact every
            # Go job restores. Digest-cached on go.mod/go.sum, so it is reused
            # across runs until dependencies change (see _GO_MOD_CACHE_DIGEST).
            # Downloading is network- not CPU-bound; the small x86 pool suffices.
            Job.Config(
                name="Go modules",
                runs_on=[RunnerLabels.AMD_8CPU_16G],
                command="python3 ci/scripts/go_mod_cache.py",
                provides=[GO_MOD_CACHE_ARTIFACT.name],
                digest_config=_GO_MOD_CACHE_DIGEST,
                timeout=20 * 60,
            ),
            # Port of flow.yml's `unit_tests` job (no services). The driver runs
            # gotestsum and emits a structured praktika Result (per-test table,
            # JUnit XML attached) instead of a bare exit-code pass/fail. Protos
            # are generated in the pre-hook; the Go toolchain + gotestsum are
            # baked into the x86_64 image (ci/infrastructure/projects.py).
            Job.Config(
                name="Unit tests (no services)",
                runs_on=[RunnerLabels.AMD_32CPU_64G],
                pre_hooks=[_RESTORE_GO_MOD, "./generate-protos.sh"],
                command="python3 ci/scripts/unit_tests.py",
                requires=[GO_MOD_CACHE_ARTIFACT.name],
                timeout=30 * 60,
            ),
            # Port of flow.yml's `test_selection_coverage` job.
            Job.Config(
                name="Test selection coverage",
                runs_on=[RunnerLabels.AMD_8CPU_16G],
                pre_hooks=[_RESTORE_GO_MOD, "./generate-protos.sh"],
                command="bash .github/scripts/check-flow-test-selection.sh",
                requires=[GO_MOD_CACHE_ARTIFACT.name],
                timeout=20 * 60,
            ),
            # Build the 3 flow images once; the matrix loads them instead of
            # each rebuilding (port of flow.yml's flow_images job).
            Job.Config(
                name="Flow images",
                runs_on=[RunnerLabels.AMD_8CPU_16G],
                pre_hooks=["./generate-protos.sh"],
                command="python3 ci/scripts/flow_images.py",
                provides=[FLOW_IMAGES_ARTIFACT.name],
                timeout=30 * 60,
            ),
            # Compile every flow + pkg test package once and publish GOCACHE as
            # an artifact the matrix restores (port of flow.yml's go_build_cache
            # job). CPU-bound compile on the big pool; it gates the matrix, so it
            # never contends with the e2e jobs for that pool.
            Job.Config(
                name="Go build cache",
                runs_on=[RunnerLabels.AMD_32CPU_64G],
                pre_hooks=[_RESTORE_GO_MOD, "./generate-protos.sh"],
                command="python3 ci/scripts/go_build_cache.py",
                provides=[GO_BUILD_CACHE_ARTIFACT.name],
                requires=[GO_MOD_CACHE_ARTIFACT.name],
                timeout=30 * 60,
            ),
            # flow.yml's flow_test matrix. Each target is resolved from
            # ci/scripts/flow_matrix.py by job name; the driver
            # ci/scripts/flow_test.py loads the prebuilt flow images, brings up
            # the docker-compose stack via Tilt and runs gotestsum on the host,
            # emitting a structured praktika Result (per-test table, diagnostics
            # attached). Blocking (no allow_failure): the matrix is green and
            # gates merge via the Ready-For-Merge status below. Cloud-destination
            # permutations stay disabled in flow_matrix until keyless auth lands.
            # One shared command for every permutation; the driver derives its
            # target from the job name (Info().job_name -> "Flow test (<label>)").
            *Job.Config(
                name="Flow test",
                runs_on=[RunnerLabels.AMD_32CPU_64G],
                command="python3 ci/scripts/flow_test.py",
                # Gate the matrix on the prep jobs: the artifacts are downloaded
                # (flow images, Go build cache, Go module cache); Rust Lint and
                # Unit tests are ordering-only gates so a broken build/lint fails
                # fast before allocating the heavy e2e pool (matches flow.yml's
                # `needs` on the flow_test job). flow_test.py restores both Go
                # caches in-driver.
                requires=[
                    FLOW_IMAGES_ARTIFACT.name,
                    GO_BUILD_CACHE_ARTIFACT.name,
                    GO_MOD_CACHE_ARTIFACT.name,
                ],
                run_after=["Rust Lint", "Unit tests (no services)"],
                timeout=60 * 60,
            ).parametrize(
                *[Job.ParamSet(parameter=label) for label in LABELS]
            ),
        ],
        artifacts=[FLOW_IMAGES_ARTIFACT, GO_BUILD_CACHE_ARTIFACT, GO_MOD_CACHE_ARTIFACT],
        enable_cache=True,
        enable_report=True,
        enable_gh_summary_comment=True,
        enable_exit_code_result=True,
        # Aggregate every (non-allow_failure) job into one "Ready For Merge [PR]"
        # commit status; make that the branch-protection required check so the
        # whole matrix gates merge.
        enable_merge_ready_status=True,
    )
]
