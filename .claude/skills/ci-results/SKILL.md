---
name: ci-results
description: Fetch and interpret Praktika CI structured JSON results for this repo (peerdb). Use whenever the user asks to investigate, address, or fix CI failures, or refers to a PR / CI report without specifying what broke. Always fetch the result JSON before reading code or proposing fixes.
---

# Praktika CI results (peerdb)

Whenever the user asks to investigate, address, or fix CI failures — or refers to a
PR without specifying what's broken — **always fetch the CI result JSON first**
before reading code or proposing fixes. The `info` field of failing nodes is the
primary signal for what went wrong.

This repo's Praktika config:

| Field | Value |
|---|---|
| Artifact/report bucket | `peerdb-artifacts-eu-west-1` |
| HTTP endpoint | `peerdb-artifacts-eu-west-1.s3.amazonaws.com` |
| AWS region / profile | `eu-west-1` / `PeerDBCI` |
| PR workflow name | `PR` → normalized `pr` |
| Main branch | `main` |

> The directory/filename segment comes from the workflow's **`name=` field**, not the
> `.py` filename. E.g. `ci/workflows/pull_request.py` declares `name="PR"`, so the
> segment is `pr` (not `pull_request`). If unsure, grep `name=` in
> `ci/workflows/*.py` and normalize per the rules below. (Currently only the `PR`
> workflow exists.)

Normalization (`Utils.normalize_string`): lowercase, then replace each of
`space ( ) { } ' [ ] , / - : " &` with `_`, collapse repeated `_`, strip trailing `_`.

(Source: `ci/settings/settings.py`, `ci/workflows/`, `praktika/utils.py`.)

## Step 1 — Determine PR/branch, sha, and workflow name

**From a bare PR number** (or the current branch):
```bash
gh pr view {PR} --json number,headRefOid,title,url --jq '{number,sha:.headRefOid,title,url}'
# omit {PR} to use the current branch's PR
```
Use workflow `pr` for PR runs.

**From a Praktika report URL** (`...praktika.html?PR=357&sha=abc123&name_0=PR`):
- Extract `PR`, `sha`, `name_0` from the query params.
- Normalize `name_0`: lowercase, spaces → underscores (`"PR"` → `"pr"`).

## Step 2 — Build the JSON URL

The result object lives under the **normalized workflow name** directory, and the
filename embeds that same normalized name.

**PR run:**
```
https://peerdb-artifacts-eu-west-1.s3.amazonaws.com/PRs/{PR}/{sha}/pr/result_pr.json
```

**Main/branch run** (push to a ref — no PR number):
```
https://peerdb-artifacts-eu-west-1.s3.amazonaws.com/REFs/{branch}/{sha}/{workflow}/result_{workflow}.json
```

Latest run for a PR/branch (sha unknown): swap the sha segment for `latest`, e.g.
`.../PRs/{PR}/latest/pr/result_pr.json`.

Fetch with WebFetch (or `curl -s`). The bucket returns **HTTP 403, not 404**, for a
missing key — so a 403 almost always means the path is wrong (bad sha, branch, or
workflow name), not an auth problem.

## Step 3 — Walk the result tree

The JSON is a serialized `praktika.Result`:

```
{
  "name": str,
  "status": str,       # OK | FAIL | ERROR | SKIPPED | UNKNOWN | XFAIL | XPASS | PENDING | RUNNING | DROPPED
  "start_time": float?,
  "duration": float?,
  "info": str,         # <-- primary failure signal
  "results": [...],    # nested Result objects, same shape, recursive
  "files": [...],
  "links": [...],
  "ext": {
    "labels": [{"name": str, "link": str?, "hint": str?}, ...],
    "warnings": [...],
    "errors": [...],
    "report_url": str?,
    ...
  }
}
```

Recurse `results` to find every node whose `status` is `FAIL` or `ERROR`, and read
its `info`. Each sub-job may also have its own `result_<normalized_job_name>.json`
artifact and log `files`/`links` worth fetching for detail.

Quick triage with `jq` after downloading:
```bash
curl -s "{json_url}" -o /tmp/result.json

# all FAIL/ERROR nodes (includes parent aggregates like "Lint: Failures 1/6")
jq -r '.. | objects | select(.status=="FAIL" or .status=="ERROR") | "\(.name): \(.status)\n\(.info)\n"' /tmp/result.json

# failed LEAVES only (no children) with info + links + files — the actionable list
jq -r '[.. | objects | select((.status=="FAIL" or .status=="ERROR") and ((.results|length)==0))][] | "### \(.name) [\(.status)]\n\(.info // "")\nlinks: \((.links // [])|join(", "))\nfiles: \((.files // [])|join(", "))\n"' /tmp/result.json
```
Leaves carry the per-job `job.log` link; fetch it for full output when `info` is truncated.

## PeerDB-specific notes

- Go e2e/connector test failures are also queryable via the `cidb` tool (see
  `.claude/CLAUDE.md` → "CI test results"): `default.ci_peerdb_test_runs` on the
  `stresshouse` instance. The result JSON tells you *which job* failed; `cidb` gives
  structured per-test traces for the Go suites.
- For local reproduction of e2e/connector failures, use the `local-debug` skill.
