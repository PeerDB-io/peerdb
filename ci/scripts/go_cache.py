#!/usr/bin/env python3
"""Shared Go cache pack/restore for the PeerDB Praktika CI.

Two run-shared Go caches, each delivered as an S3 artifact:

  mod   -> GOMODCACHE (`go env GOMODCACHE`): downloaded module source. Keyed on
           go.mod/go.sum via the producer job's digest_config, so Praktika reuses
           it across runs until dependencies change -- the persisted, go.sum-keyed
           module cache GHA's setup-go gave us. It is arch/OS independent, so one
           bundle serves every consumer and kills the per-job cold `go mod
           download` (~21s).

  build -> GOCACHE (`go env GOCACHE`): compiled package/test objects. Run-scoped
           (any .go edit changes it), so it is rebuilt each run by the Go build
           cache job and restored by the flow-test matrix.

Producers call pack(); consumers call restore(). Caches are extracted into the
consumer's own `go env` locations (the defaults), so no `go env -w` is needed and
differing $HOME between producer and consumer does not matter. restore() is
best-effort: a missing/corrupt artifact just means the job does the work cold.

CLI (for use as a job pre_hook / command):
    go_cache.py restore mod|build
    go_cache.py pack    mod|build <out.tar.zst>
"""
import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.getcwd())

from praktika.settings import Settings  # noqa: E402

ARCHIVES = {"mod": "go-mod-cache.tar.zst", "build": "go-build-cache.tar.zst"}
_ENV_VAR = {"mod": "GOMODCACHE", "build": "GOCACHE"}


def _cache_dir(kind):
    return subprocess.check_output(["go", "env", _ENV_VAR[kind]], text=True).strip()


def _run(cmd):
    print(f"+ {cmd}", flush=True)
    return subprocess.run(cmd, shell=True).returncode


def pack(kind, out):
    """Tar the kind's cache dir into ``out`` (zstd). Returns a shell return code."""
    cache_dir = _cache_dir(kind)
    os.makedirs(cache_dir, exist_ok=True)
    rc = _run(f"tar -C {cache_dir} -cf - . | zstd -T0 -3 > {out}")
    _run(f"ls -lh {out} || true")
    return rc


def restore(kind):
    """Extract the kind's artifact (if present in INPUT_DIR) into its cache dir.
    Best effort: always returns 0 so a missing/broken cache never fails the job."""
    archive = os.path.join(Settings.INPUT_DIR, ARCHIVES[kind])
    if not os.path.exists(archive):
        print(f"no {kind} cache artifact ({archive}); job runs cold", flush=True)
        return 0
    cache_dir = _cache_dir(kind)
    os.makedirs(cache_dir, exist_ok=True)
    print(f"restoring {kind} cache into {cache_dir}", flush=True)
    if _run(f"zstd -d -T0 < {archive} | tar -x -C {cache_dir}") != 0:
        print(f"WARNING: failed to restore {kind} cache; job runs cold", flush=True)
    return 0


def main(argv):
    if len(argv) == 2 and argv[0] == "restore" and argv[1] in ARCHIVES:
        return restore(argv[1])
    if len(argv) == 3 and argv[0] == "pack" and argv[1] in ARCHIVES:
        return pack(argv[1], argv[2])
    raise SystemExit(
        "usage: go_cache.py restore mod|build | pack mod|build <out.tar.zst>"
    )


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
