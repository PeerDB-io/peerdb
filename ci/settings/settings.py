# Runner labels follow the `<arch>_<Ncpu>_<Mg>` convention: architecture,
# vCPU count, and RAM in GiB. The label value doubles as the pool / SQS queue
# name in ci/infrastructure/projects.py.
class RunnerLabels:
    # c8g.large — rust-lint and other light arm jobs.
    ARM_2CPU_4G = "arm_2cpu_4g"
    # c8i.8xlarge — heavy x86_64 Tilt e2e pool (32 vCPU / 64 GiB, 1:2). Matches
    # GitHub's ubuntu-latest-32-cores on CPU; RAM isn't the constraint in CI, so
    # 1:2 for now (go to m-family if we ever see memory pressure). Unit tests +
    # flow_test matrix.
    AMD_32CPU_64G = "amd_32cpu_64g"
    # c8i.2xlarge — smaller x86_64 pool for the flow-image build + test-selection
    # (jobs that can't saturate the big pool).
    AMD_8CPU_16G = "amd_8cpu_16g"


PROJECT_NAME = "peerdb"
PROJECT_SLUG = "peerdb"
MAIN_BRANCH = "main"

CI_CONFIG_RUNS_ON = [RunnerLabels.ARM_2CPU_4G]

AWS_REGION = "eu-west-1"
AWS_PROFILE = "PeerDBCI"

S3_ARTIFACT_BUCKET = f"{PROJECT_SLUG}-artifacts-{AWS_REGION}"
S3_REPORT_BUCKET = S3_ARTIFACT_BUCKET
CACHE_S3_PATH = f"{S3_ARTIFACT_BUCKET}/ci_cache"
ENABLE_SUBMODULE_CACHE = True

# Snapshot the repo to S3 once (Config Workflow) so downstream jobs restore instead
# of cloning; for PRs, snapshot the ephemeral merge of head into the target tip.
ENABLE_S3_REPO_SNAPSHOT = False
ENABLE_PR_EPHEMERAL_MERGE_COMMIT = False
S3_BUCKET_TO_HTTP_ENDPOINT = {
    S3_REPORT_BUCKET: f"{S3_REPORT_BUCKET}.s3.amazonaws.com",
}

GH_AUTH_LAMBDA_NAME = f"{PROJECT_SLUG}-gh-token"
GH_AUTH_LAMBDA_REGION = AWS_REGION
PRAKTIKA_BASE_VENV = "praktika-runtime-0.0.1"

# GitHub Checks API rate-limit tuning — raise these on very large CI
# (many concurrent PRs sharing one GitHub App installation's rate-limit
# bucket). See docs/gh-app-rate-limits.md.
#   Minimum seconds between top-level workflow-check PATCHes (the check
#   is also only PATCHed when its body actually changed):
TOP_CHECK_MIN_PATCH_INTERVAL_S = 20
#   Per-job check-runs: "verbose" posts one per job through its full
#   lifecycle; "minimal" posts a check-run only for jobs that fail.
JOB_CHECKS_MODE = "verbose"
VALIDATE_FILE_PATHS = False
