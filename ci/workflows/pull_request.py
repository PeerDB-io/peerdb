from praktika import Job, Workflow
from ci.settings.settings import RunnerLabels


WORKFLOWS = [
    Workflow.Config(
        name="Pull Request CI",
        event=Workflow.Event.PULL_REQUEST,
        base_branches=["main"],

        jobs=[
            Job.Config(
                name="Smoke Test",
                runs_on=[RunnerLabels.SMALL_ARM],
                command='python3 -c \'print("hello from praktika")\'',
            ),
            # Port of .github/workflows/rust-lint.yml. The Rust toolchain, buf
            # and protoc are baked into the runner image (see
            # ci/infrastructure/projects.py); only the gitignored generated
            # protobuf bindings are produced per run, in the pre-hook.
            Job.Config(
                name="Rust Lint",
                runs_on=[RunnerLabels.SMALL_ARM],
                pre_hooks=["buf generate protos"],
                command="cd nexus && cargo clippy -- -D warnings",
                timeout=60 * 60,
            ),
        ],
        enable_cache=True,
        enable_report=True,
        enable_gh_summary_comment=True,
        enable_exit_code_result=True,
    )
]
