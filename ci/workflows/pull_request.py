import os
import sys

from praktika import Artifact, Job, Workflow
from ci.settings.settings import RunnerLabels

# flow_matrix / flow_filter live under ci/scripts (the job scripts dir).
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
from flow_matrix import LABELS  # noqa: E402
from flow_filter import should_skip_flow_test  # noqa: E402

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

# A digest doubles as Praktika's PR change-filter: a job runs only when a
# changed file matches its include_paths, else it is SKIPPED (and SKIPPED does
# not block merge-ready). A job's `requires` already pulls in its producers'
# digests, so never repeat a path a required artifact already covers.

# Reuse the module cache until deps or the producer logic move.
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

# Not self-correcting (bundles a compiled binary): key everything affecting it.
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

# Lint: union of rust-lint (nexus), golang-lint (flow, e2e_cleanup) and ui-lint.
_LINT_DIGEST = Job.CacheDigestConfig(
    include_paths=[
        "./nexus",
        "./flow",
        "./e2e_cleanup",
        "./ui",
        "./ci/scripts/lint.py",
        *_PROTO_INPUTS,
    ],
)

# ui-build.yml paths.
_UI_BUILD_DIGEST = Job.CacheDigestConfig(
    include_paths=["./ui", *_PROTO_INPUTS],
)

# check-renovate-config.yml paths (its old `paths:` filter).
_RENOVATE_DIGEST = Job.CacheDigestConfig(
    include_paths=[
        "./renovate.json5",
        "./scripts/renovate-simulation.sh",
    ],
)

# Go jobs over the flow module (nexus is gated by Lint's clippy, not these).
_UNIT_TESTS_DIGEST = Job.CacheDigestConfig(
    include_paths=["./flow", "./ci/scripts/unit_tests.py", *_PROTO_INPUTS],
)
_CATALOG_MIGRATE_DIGEST = Job.CacheDigestConfig(
    include_paths=["./flow", "./ci/scripts/catalog_migrate.py", *_PROTO_INPUTS],
)
# Also re-run when the package/test mapping moves.
_TEST_SELECTION_DIGEST = Job.CacheDigestConfig(
    include_paths=[
        "./flow",
        "./.github/scripts/check-flow-test-selection.sh",
        "./.github/scripts/select-flow-tests.sh",
        "./ci/scripts/flow_matrix.py",
        *_PROTO_INPUTS,
    ],
)

# Flow test: only the stack + drivers. Everything else (./flow incl. e2e test
# code, protos) rides in via the required flow-images / go-build-cache artifacts,
# so a non-flow PR skips the matrix and the now-unrescued producers skip too.
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


# Consumer pre-hook: restore the shared module cache before any Go work.
_RESTORE_GO_MOD = "python3 ci/scripts/go_cache.py restore mod"

# Postgres majors ci.yml exercises the catalog migrations against.
_NEXUS_PG_VERSIONS = ["14", "15", "16", "17", "18"]


WORKFLOWS = [
    Workflow.Config(
        name="PR",
        event=Workflow.Event.PULL_REQUEST,
        base_branches=["main"],

        jobs=[
            # Native Praktika AI code review (replaces the GHA claude-code-review
            # workflows). Invokes a Bedrock model, posts a summary + inline
            # findings and manages its own threads. Runs on the arm pool (granted
            # bedrock:InvokeModel in projects.py). allow_failure so a review
            # hiccup never blocks merge; enable_gh_auth to post/resolve comments.
            # NOTE: needs the Bedrock model enabled for the account in AWS_REGION;
            # adjust --model if the inference profile differs.
            # No digest_config: must run on every PR (incl. Renovate).
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
            # Consolidated linters: rust-lint.yml (nexus clippy) + golang-lint.yml
            # + the lint half of ui-lint.yml. Toolchains baked in; protos in the
            # pre-hook.
            Job.Config(
                name="Lint",
                runs_on=[RunnerLabels.AMD_8CPU_16G],
                pre_hooks=[_RESTORE_GO_MOD, "./generate-protos.sh"],
                command="python3 ci/scripts/lint.py",
                requires=[GO_MOD_CACHE_ARTIFACT.name],
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
            # Port of check-renovate-config.yml: validate renovate.json5 and
            # simulate a Renovate run (needs node/npx + jq, baked in, and a
            # github.com token for the shared renovate-config extension — hence
            # enable_gh_auth). Digest-gated to the renovate config + script, so it
            # only runs when those change (its old GHA `paths:` filter).
            # allow_failure: the local Renovate run is network-heavy; don't block
            # merge on a flaky sim. (The GHA PR-comment report is not ported yet.)
            Job.Config(
                name="Renovate config",
                runs_on=[RunnerLabels.AMD_8CPU_16G],
                command="export GITHUB_COM_TOKEN=$(gh auth token) && ./scripts/renovate-simulation.sh",
                enable_gh_auth=True,
                digest_config=_RENOVATE_DIGEST,
                allow_failure=True,
                timeout=20 * 60,
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
            # Port of flow.yml's `unit_tests` job (no services); gotestsum ->
            # structured Result. Protos in the pre-hook.
            Job.Config(
                name="Unit tests (no services)",
                runs_on=[RunnerLabels.AMD_32CPU_64G],
                pre_hooks=[_RESTORE_GO_MOD, "./generate-protos.sh"],
                command="python3 ci/scripts/unit_tests.py",
                requires=[GO_MOD_CACHE_ARTIFACT.name],
                digest_config=_UNIT_TESTS_DIGEST,
                timeout=30 * 60,
            ),
            # Port of flow.yml's `test_selection_coverage` job.
            Job.Config(
                name="Test selection coverage",
                runs_on=[RunnerLabels.AMD_8CPU_16G],
                pre_hooks=[_RESTORE_GO_MOD, "./generate-protos.sh"],
                command="bash .github/scripts/check-flow-test-selection.sh",
                requires=[GO_MOD_CACHE_ARTIFACT.name],
                digest_config=_TEST_SELECTION_DIGEST,
                timeout=20 * 60,
            ),
            # Port of ci.yml's catalog-migration check: `go run . migrate` against
            # Postgres 14-18 (fresh container per version). ci.yml's nexus cargo
            # check is covered by Lint's clippy; cargo test needs cloud creds.
            *Job.Config(
                name="Catalog migrate",
                runs_on=[RunnerLabels.AMD_32CPU_64G],
                pre_hooks=[_RESTORE_GO_MOD, "./generate-protos.sh"],
                command="python3 ci/scripts/catalog_migrate.py",
                requires=[GO_MOD_CACHE_ARTIFACT.name],
                digest_config=_CATALOG_MIGRATE_DIGEST,
                timeout=20 * 60,
            ).parametrize(
                *[Job.ParamSet(parameter=f"pg-{v}") for v in _NEXUS_PG_VERSIONS]
            ),
            # Build the 3 flow images once; the matrix loads them (port of
            # flow.yml's flow_images job).
            Job.Config(
                name="Flow images",
                runs_on=[RunnerLabels.AMD_8CPU_16G],
                pre_hooks=["./generate-protos.sh"],
                command="python3 ci/scripts/flow_images.py",
                provides=[FLOW_IMAGES_ARTIFACT.name],
                digest_config=_FLOW_IMAGES_DIGEST,
                timeout=30 * 60,
            ),
            # Compile every flow + pkg test package once and publish GOCACHE as
            # an artifact the matrix restores (port of flow.yml's go_build_cache
            # job).
            Job.Config(
                name="Go build cache",
                runs_on=[RunnerLabels.AMD_8CPU_16G],
                pre_hooks=[_RESTORE_GO_MOD, "./generate-protos.sh"],
                command="python3 ci/scripts/go_build_cache.py",
                provides=[GO_BUILD_CACHE_ARTIFACT.name],
                requires=[GO_MOD_CACHE_ARTIFACT.name],
                digest_config=_GO_BUILD_CACHE_DIGEST,
                timeout=30 * 60,
            ),
            # flow.yml's flow_test matrix. The driver (flow_test.py) resolves its
            # target from the job name via flow_matrix.py, loads the prebuilt
            # images, brings up the Tilt/compose stack and runs gotestsum.
            # Blocking: gates merge via the Ready-For-Merge status below.
            *Job.Config(
                name="Flow test",
                runs_on=[RunnerLabels.AMD_32CPU_64G],
                command="python3 ci/scripts/flow_test.py",
                # Download the prep artifacts; Rust Lint + Unit tests are
                # ordering-only gates so a broken build fails fast before the
                # heavy e2e pool. flow_test.py restores both Go caches in-driver.
                requires=[
                    FLOW_IMAGES_ARTIFACT.name,
                    GO_BUILD_CACHE_ARTIFACT.name,
                    GO_MOD_CACHE_ARTIFACT.name,
                ],
                run_after=["Rust Lint", "Unit tests (no services)"],
                digest_config=_FLOW_TEST_DIGEST,
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
        # Skip jobs whose digest include_paths don't match the PR's changed
        # files (a non-flow PR skips the Go/flow jobs; a docs-only PR skips
        # almost everything). The `ci-force-all` label bypasses this.
        enable_job_filtering_by_changes=True,
        # Per-setup flow-matrix skipping: drop a permutation when the PR's flow
        # changes are confined to other setups (ci/scripts/flow_filter.py).
        workflow_filter_hooks=[should_skip_flow_test],
        # Aggregate every (non-allow_failure) job into one "Ready For Merge [PR]"
        # commit status; make that the branch-protection required check so the
        # whole matrix gates merge.
        enable_merge_ready_status=True,
    )
]
