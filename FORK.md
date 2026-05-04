# Fork policy

## Background

Peerflake is a community fork of [PeerDB](https://github.com/PeerDB-io/peerdb), maintained independently as a non-commercial open-source project. The fork exists to:

- Qualify for [LocalStack's free OSS license](https://www.localstack.cloud/localstack-open-source) to enable Snowflake destination testing without a paid Snowflake account.
- Provide a non-commercial home for community contributions, especially around testing infrastructure (LocalStack Snowflake emulator integration).

## Goals

- Track upstream PeerDB closely
- Stay byte-identical internally to enable easy syncing and upstreaming
- Maintain AGPLv3 compliance
- Enable LocalStack-based Snowflake emulator testing in CI

## What's different from upstream

- Repository name, branding, Docker image name
- Additional CI workflows for LocalStack-based testing
- This `FORK.md`, `NOTICE`, and updated `README.md`

Everything else should match upstream.

## Sync schedule

Upstream PeerDB is merged into `main` on demand (weekly or when important fixes land).

```bash
# Weekly sync
git fetch upstream
git checkout main
git merge upstream/main
# Conflicts should be minimal — only in README, NOTICE, Dockerfile,
# docker-bake.hcl, and CI workflow files (the things we renamed).
# Resolve by keeping fork's branding and upstream's logic.
git push origin main
```

## Contribution flow

1. Bug fixes and improvements developed here are upstreamed to PeerDB when applicable (clean diffs, no rename noise).
2. Fork-specific changes (CI, LocalStack integration, branding) live only in this repo.

## Upstreaming workflow

```bash
# Develop fix on a feature branch
git checkout -b fix/some-bug
# ... make changes to internal code (which is identical to upstream) ...
git commit

# Push to upstream PeerDB
git push origin fix/some-bug
# Open PR against PeerDB-io/peerdb on GitHub
```

Because internal naming is unchanged, the diff is clean and ready for upstream review.
