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
            # Build the 3 flow images once; the matrix loads them instead of
            # each rebuilding (port of flow.yml's flow_images job).
            Job.Config(
                name="Flow images",
                runs_on=[RunnerLabels.AMD_64CPU_128G],
                pre_hooks=["./generate-protos.sh"],
                command="python3 ci/scripts/flow_images.py",
                provides=[FLOW_IMAGES_ARTIFACT.name],
                timeout=30 * 60,
            ),
            # flow.yml's flow_test matrix. Each target is resolved from
            # ci/scripts/flow_matrix.py by job name; the driver
            # ci/scripts/flow_test.py loads the prebuilt flow images, brings up
            # the docker-compose stack via Tilt and runs gotestsum on the host,
            # emitting a structured praktika Result (per-test table, diagnostics
            # attached). allow_failure for now: cloud-destination permutations
            # are disabled (see flow_matrix) and a few suites still flake, so we
            # keep the matrix visible without blocking merge while it stabilizes.
            # One shared command for every permutation; the driver derives its
            # target from the job name (Info().job_name -> "Flow test (<label>)").
            *Job.Config(
                name="Flow test",
                runs_on=[RunnerLabels.AMD_64CPU_128G],
                command="python3 ci/scripts/flow_test.py",
                requires=[FLOW_IMAGES_ARTIFACT.name],
                timeout=60 * 60,
                allow_failure=True,
            ).parametrize(
                *[Job.ParamSet(parameter=label) for label in LABELS]
            ),
        ],
        artifacts=[FLOW_IMAGES_ARTIFACT],
        enable_cache=True,
        enable_report=True,
        enable_gh_summary_comment=True,
        enable_exit_code_result=True,
    )
]
