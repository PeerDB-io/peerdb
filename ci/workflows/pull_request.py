import os
import sys

from praktika import Artifact, Job, Workflow
from praktika.settings import Settings
from ci.settings.settings import RunnerLabels

# flow_matrix / flow_filter live under ci/scripts (the job scripts dir).
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
from flow_matrix import LABELS  # noqa: E402
from flow_filter import should_skip_flow_test  # noqa: E402

# flow-api/worker/snapshot-worker built once, shared with the matrix as a
# docker-save+zstd tarball (ci/scripts/flow_images.py; loader in flow_test.py).
FLOW_IMAGES_ARTIFACT = Artifact.Config(
    name="flow-images",
    type=Artifact.Type.S3,
    path="flow-images.tar.zst",
)

# Every flow test package compiled once, shared as a GOCACHE tarball
# (ci/scripts/go_build_cache.py; loader in flow_test.py).
GO_BUILD_CACHE_ARTIFACT = Artifact.Config(
    name="go-build-cache",
    type=Artifact.Type.S3,
    path="go-build-cache.tar.zst",
)

# The GOMODCACHE is a self-healing, content-addressed S3 cache (ci/scripts/
# go_cache.py), not a Praktika artifact: there is no producer job. Every Go job's
# `restore mod` pre-hook downloads the go.sum-keyed bundle if present, else runs
# `go mod download` cold, and its `save mod` post-hook publishes the bundle when it
# built cold and no sibling has yet -- the GHA setup-go model. So the module cache
# is never a DAG node the critical path waits on.

# A job's digest doubles as Praktika's PR change-filter: it runs only when a
# changed file matches include_paths, else SKIPPED (which doesn't block merge).
# `requires` already pulls in producers' digests, so don't repeat their paths.
# GOCACHE is content-addressed, so an imperfect key only costs a recompile.
_GO_BUILD_CACHE_DIGEST = Job.CacheDigestConfig(
    include_paths=[
        "./flow",
        "./protos",
        "./.github/scripts/select-flow-tests.sh",
        "./ci/scripts/go_build_cache.py",
        "./ci/scripts/go_cache.py",
    ],
)

# Bundles a compiled binary (not self-correcting): key everything affecting it.
_FLOW_IMAGES_DIGEST = Job.CacheDigestConfig(
    include_paths=[
        "./flow",
        "./protos",
        "./buf.gen.yaml",
        "./generate-protos.sh",
        "./stacks/flow.Dockerfile",
        "./stacks/build-flow-images.sh",
        "./ci/scripts/flow_images.py",
    ],
)

# Generated code is a shared input, so every Go/TS job keys on the proto inputs.
_PROTO_INPUTS = ["./protos", "./buf.gen.yaml", "./generate-protos.sh"]

_LINT_DIGEST = Job.CacheDigestConfig(
    include_paths=[
        "./nexus",
        "./flow",
        "./e2e_cleanup",
        "./ui",
        "./ci/scripts/lint.py",
        "./ci/scripts/go_cache.py",
        *_PROTO_INPUTS,
    ],
)

_UI_BUILD_DIGEST = Job.CacheDigestConfig(
    include_paths=["./ui", *_PROTO_INPUTS],
)

_RENOVATE_DIGEST = Job.CacheDigestConfig(
    include_paths=[
        "./renovate.json5",
        "./scripts/renovate-simulation.sh",
    ],
)

_UNIT_TESTS_DIGEST = Job.CacheDigestConfig(
    include_paths=[
        "./flow",
        "./ci/scripts/unit_tests.py",
        "./ci/scripts/go_cache.py",
        *_PROTO_INPUTS,
    ],
)
_CATALOG_MIGRATE_DIGEST = Job.CacheDigestConfig(
    include_paths=[
        "./flow",
        "./ci/scripts/catalog_migrate.py",
        "./ci/scripts/go_cache.py",
        *_PROTO_INPUTS,
    ],
)
_TEST_SELECTION_DIGEST = Job.CacheDigestConfig(
    include_paths=[
        "./flow",
        "./.github/scripts/check-flow-test-selection.sh",
        "./.github/scripts/select-flow-tests.sh",
        "./ci/scripts/flow_matrix.py",
        "./ci/scripts/go_cache.py",
        *_PROTO_INPUTS,
    ],
)

# Only the stack + drivers; ./flow + protos ride in via the required
# flow-images / go-build-cache artifacts, so a non-flow PR skips the matrix.
_FLOW_TEST_DIGEST = Job.CacheDigestConfig(
    include_paths=[
        "./Tiltfile",
        "./docker-compose-dev.yml",
        "./ancillary-docker-compose.yml",
        "./stacks",
        "./ci/scripts/flow_test.py",
        "./ci/scripts/tilt_env.py",
        "./ci/scripts/flow_matrix.py",
    ],
)


# Every Go job restores the shared module cache before its work (pre-hook,
# downloads-or-builds) and publishes it at the end if it built cold (post-hook,
# no-op on a hit / when a sibling already published). Together these are the
# self-healing mod cache -- no producer job (see GO mod cache note above).
_RESTORE_GO_MOD = "python3 ci/scripts/go_cache.py restore mod"
_SAVE_GO_MOD = "python3 ci/scripts/go_cache.py save mod"

# Postgres majors ci.yml exercises the catalog migrations against.
_NEXUS_PG_VERSIONS = ["14", "15", "16", "17", "18"]

# Cortages (praktika/docs/cortage-jobs.md) pin a chain of jobs to one runner /
# checkout / ci/tmp and run them back-to-back with no cleanup, so the critical
# path pays per-job overhead (queue, boot, checkout, artifact download) once and a
# later member reads an earlier member's output from ci/tmp instead of S3. Three
# chains cover the path to the matrix, each ending in one of the three longest
# perms (ci/praktika-vs-gha-critical-path.md §2), all on amd_32cpu_64g:
#   1. Flow images -> Flow test (pg17)
#   2. Config Workflow -> Go build cache -> Flow test (pg16, long pole)
#   3. Unit tests -> Flow test (pg18)
# Chain 2 is the producer spine for GOCACHE: Go build cache compiles once (its
# restore pre-hook warms the module cache from the self-healing S3 mod cache, or
# builds it cold) and ships GOCACHE that pg16 reads from ci/tmp. It promotes onto
# amd_32cpu_64g from its native Config head; 1 and 3 are already headed by a 32-core
# job. The build cache producer still uploads GOCACHE to S3, so the other matrix
# perms read it normally. No flow test gates on Lint: it runs on a separate, slower
# pool and gates nothing the e2e needs, so waiting on it only delays the matrix
# (they keep the Unit tests gate).
_LONGEST_FLOW_LABEL = "postgres-16_ch-lts"
_SECOND_FLOW_LABEL = "postgres-17_ch-stable"
_THIRD_FLOW_LABEL = "postgres-18_ch-latest"

# flow.yml's flow_test matrix: flow_test.py resolves the target from the job name,
# loads the prebuilt images, brings up the Tilt stack and runs gotestsum. Gates
# merge via Ready-For-Merge. Parametrized per enabled target; the three longest
# perms are pulled out and chained into cortages below.
_FLOW_TEST_BASE = Job.Config(
    name="Flow test",
    runs_on=[RunnerLabels.AMD_32CPU_64G],
    command="python3 ci/scripts/flow_test.py",
    # Unit tests is an ordering-only gate (fail fast before the heavy e2e pool);
    # flow_test.py restores the build cache artifact + the self-healing mod cache
    # in-driver.
    requires=[
        FLOW_IMAGES_ARTIFACT.name,
        GO_BUILD_CACHE_ARTIFACT.name,
    ],
    run_after=["Unit tests (no services)"],
    digest_config=_FLOW_TEST_DIGEST,
    timeout=60 * 60,
)


WORKFLOWS = [
    Workflow.Config(
        name="PR",
        event=Workflow.Event.PULL_REQUEST,
        base_branches=["main"],

        jobs=[
            # Native Praktika AI review (replaces the GHA claude-code-review
            # workflows): a Bedrock model posts a summary + inline findings and
            # manages its own threads. Runs on every PR (no digest filter).
            # allow_failure so a hiccup never blocks merge; enable_gh_auth to
            # post/resolve. NOTE: needs the Bedrock model enabled in AWS_REGION.
            Job.Config(
                name="Code Review",
                runs_on=[RunnerLabels.ARM_2CPU_4G],
                command=(
                    "python3 -I -m praktika review --provider bedrock-openai "
                    "--model global.openai.gpt-5.6-sol --reasoning-effort high "
                    "--prompt ./ci/prompts/code_review.md --fail-for-draft-pr"
                ),
                allow_failure=True,
                enable_gh_auth=True,
            ),
            # Consolidated linters: rust-lint (nexus clippy) + golang-lint + the
            # lint half of ui-lint. Toolchains baked in; protos in the pre-hook.
            Job.Config(
                name="Lint",
                runs_on=[RunnerLabels.AMD_8CPU_16G],
                pre_hooks=[_RESTORE_GO_MOD, "./generate-protos.sh"],
                post_hooks=[_SAVE_GO_MOD],
                command="python3 ci/scripts/lint.py",
                digest_config=_LINT_DIGEST,
                timeout=30 * 60,
            ),
            # Port of ui-build.yml: next build.
            Job.Config(
                name="UI build",
                runs_on=[RunnerLabels.AMD_8CPU_16G],
                pre_hooks=["./generate-protos.sh"],
                command="cd ui && npm ci && npm run build",
                digest_config=_UI_BUILD_DIGEST,
                timeout=20 * 60,
            ),
            # Port of check-renovate-config.yml: validate + simulate a Renovate run
            # (needs a github.com token -> enable_gh_auth). Digest-gated to its
            # config/script; allow_failure since the sim is network-heavy and flaky.
            Job.Config(
                name="Renovate config",
                runs_on=[RunnerLabels.AMD_8CPU_16G],
                command="export GITHUB_COM_TOKEN=$(gh auth token) && ./scripts/renovate-simulation.sh",
                enable_gh_auth=True,
                digest_config=_RENOVATE_DIGEST,
                allow_failure=True,
                timeout=20 * 60,
            ),
            # Port of flow.yml's unit_tests (no services). Cortage 3 head; the 3rd
            # perm chains onto it. Its restore pre-hook warms GOMODCACHE from the
            # self-healing S3 mod cache (or builds it cold + publishes via the save
            # post-hook); the restore untar is skipped when a prior job already
            # warmed this runner's GOMODCACHE.
            Job.Config(
                name="Unit tests (no services)",
                runs_on=[RunnerLabels.AMD_32CPU_64G],
                pre_hooks=[_RESTORE_GO_MOD, "./generate-protos.sh"],
                post_hooks=[_SAVE_GO_MOD],
                command="python3 ci/scripts/unit_tests.py",
                digest_config=_UNIT_TESTS_DIGEST,
                timeout=30 * 60,
            ),
            # Port of flow.yml's test_selection_coverage.
            Job.Config(
                name="Test selection coverage",
                runs_on=[RunnerLabels.AMD_8CPU_16G],
                pre_hooks=[_RESTORE_GO_MOD, "./generate-protos.sh"],
                post_hooks=[_SAVE_GO_MOD],
                command="bash .github/scripts/check-flow-test-selection.sh",
                digest_config=_TEST_SELECTION_DIGEST,
                timeout=20 * 60,
            ),
            # Port of ci.yml's catalog check: `go run . migrate` vs Postgres 14-18
            # (fresh container per version). nexus cargo check rides Lint's clippy.
            *Job.Config(
                name="Catalog migrate",
                runs_on=[RunnerLabels.AMD_8CPU_16G],
                pre_hooks=[_RESTORE_GO_MOD, "./generate-protos.sh"],
                post_hooks=[_SAVE_GO_MOD],
                command="python3 ci/scripts/catalog_migrate.py",
                digest_config=_CATALOG_MIGRATE_DIGEST,
                timeout=20 * 60,
            ).parametrize(
                *[Job.ParamSet(parameter=f"pg-{v}") for v in _NEXUS_PG_VERSIONS]
            ),
            # Build the 3 flow images once; the matrix loads them (port of
            # flow.yml's flow_images job). Cortage 1 head (32-core); the 2nd perm
            # chains onto it. No module cache needed — it builds in clean Docker.
            Job.Config(
                name="Flow images",
                runs_on=[RunnerLabels.AMD_32CPU_64G],
                pre_hooks=["./generate-protos.sh"],
                command="python3 ci/scripts/flow_images.py",
                provides=[FLOW_IMAGES_ARTIFACT.name],
                digest_config=_FLOW_IMAGES_DIGEST,
                timeout=30 * 60,
            ),
            # Compile every flow + pkg test package once, publish GOCACHE as an
            # artifact the matrix restores (port of flow.yml's go_build_cache job).
            # Cortage 2 member: Config Workflow -> Go build cache -> Flow test
            # (longest perm), all on one runner. attach_to pulls the native Config
            # job in front (it is mangle-injected and can't attach itself);
            # promote_runs_on pins the chain to amd_32cpu_64g. Its _RESTORE_GO_MOD
            # pre-hook warms GOMODCACHE from the self-healing S3 mod cache (or builds
            # it cold + publishes via the save post-hook), then compilation runs.
            Job.Config(
                name="Go build cache",
                runs_on=[RunnerLabels.AMD_32CPU_64G],
                pre_hooks=[_RESTORE_GO_MOD, "./generate-protos.sh"],
                post_hooks=[_SAVE_GO_MOD],
                command="python3 ci/scripts/go_build_cache.py",
                provides=[GO_BUILD_CACHE_ARTIFACT.name],
                digest_config=_GO_BUILD_CACHE_DIGEST,
                timeout=30 * 60,
            ).attach_to(
                [Settings.CI_CONFIG_JOB_NAME], promote_runs_on=True
            ),
            # Flow test matrix (see _FLOW_TEST_BASE), minus the three perms chained
            # into cortages below.
            *_FLOW_TEST_BASE.parametrize(
                *[
                    Job.ParamSet(parameter=label)
                    for label in LABELS
                    if label
                    not in (_LONGEST_FLOW_LABEL, _SECOND_FLOW_LABEL, _THIRD_FLOW_LABEL)
                ]
            ),
            # Cortage 2 tail: rides the producer spine, reading GOCACHE (and the
            # module cache Go build cache's restore pre-hook warmed) from ci/tmp.
            _FLOW_TEST_BASE.parametrize(
                Job.ParamSet(parameter=_LONGEST_FLOW_LABEL)
            )[0].attach_to("Go build cache"),
            # Cortage 1 tail: reads the flow-images tarball from ci/tmp (Flow
            # images); the Go caches come from cortage 2 via S3.
            _FLOW_TEST_BASE.parametrize(
                Job.ParamSet(parameter=_SECOND_FLOW_LABEL)
            )[0].attach_to("Flow images"),
            # Cortage 3 tail: the inherited run_after=Unit tests gate matches its
            # chain predecessor; the Go caches come from cortage 2 via S3.
            _FLOW_TEST_BASE.parametrize(
                Job.ParamSet(parameter=_THIRD_FLOW_LABEL)
            )[0].attach_to("Unit tests (no services)"),
        ],
        artifacts=[FLOW_IMAGES_ARTIFACT, GO_BUILD_CACHE_ARTIFACT],
        enable_cache=True,
        enable_report=True,
        enable_gh_summary_comment=True,
        enable_exit_code_result=True,
        # Skip jobs whose digest paths don't match the PR's changes (ci-force-all
        # bypasses).
        enable_job_filtering_by_changes=True,
        # Per-setup flow-matrix skipping (ci/scripts/flow_filter.py).
        workflow_filter_hooks=[should_skip_flow_test],
        # Aggregate non-allow_failure jobs into the required "Ready For Merge [PR]"
        # status.
        enable_merge_ready_status=True,
    )
]
