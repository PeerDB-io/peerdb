#!/usr/bin/env python3
"""Download the Go modules once and publish GOMODCACHE as a Praktika artifact.

Kills the per-job cold `go mod download` (~21s): every Go job restores this
bundle instead of re-fetching. The workflow digest-caches this job on
go.mod/go.sum (see pull_request.py), so Praktika reuses the artifact across runs
until dependencies change -- the persisted, go.sum-keyed module cache GHA's
setup-go provided. GOMODCACHE is downloaded module source, so it is arch/OS
independent and shared by every consumer.

Provides the `go-mod-cache` artifact; consumers restore it via
`ci/scripts/go_cache.py restore mod` (see pull_request.py pre_hooks /
ci/scripts/flow_test.py).
"""
import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.getcwd())

from praktika.result import Result  # noqa: E402
from praktika.utils import Utils  # noqa: E402

import go_cache  # noqa: E402

ARCHIVE = go_cache.ARCHIVES["mod"]


def _run(cmd, **kwargs):
    print(f"+ {cmd}", flush=True)
    return subprocess.run(cmd, shell=True, **kwargs).returncode


def _ok(rc):
    return Result.Status.OK if rc == 0 else Result.Status.FAIL


def main():
    sw = Utils.Stopwatch()
    results = []

    sw_d = Utils.Stopwatch()
    rc = _run("go mod download", cwd="flow")
    rc = _run("go mod download", cwd="flow/pkg") or rc
    results.append(Result.create_from(
        name="go mod download", status=_ok(rc), stopwatch=sw_d,
    ))

    if rc == 0:
        sw_p = Utils.Stopwatch()
        rc_p = go_cache.pack("mod", ARCHIVE)
        results.append(Result.create_from(
            name="pack GOMODCACHE", status=_ok(rc_p), stopwatch=sw_p,
        ))

    Result.create_from(results=results, stopwatch=sw).complete_job()


if __name__ == "__main__":
    main()
