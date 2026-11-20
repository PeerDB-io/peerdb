class RunnerLabels:
    SMALL_ARM = "arm-small"
    SMALL_AMD = "amd-small"
    MEDIUM_ARM = "arm-medium"
    MEDIUM_AMD = "amd-medium"


PROJECT_NAME = "peerdb"
PROJECT_SLUG = "peerdb"
MAIN_BRANCH = "main"

CI_CONFIG_RUNS_ON = [RunnerLabels.SMALL_ARM]

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

