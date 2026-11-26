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

restore("mod") skips the untar when the GOMODCACHE is already populated for the
current go.mod/go.sum (stamped by a prior pack/restore on the same runner) -- the
common case on a cortage, where members share one GOMODCACHE with no cleanup, so
re-extracting the ~800 MB tarball on every member is pure waste.

CLI (for use as a job pre_hook / command):
    go_cache.py restore mod|build
    go_cache.py pack    mod|build <out.tar.zst>
"""
import hashlib
import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.getcwd())

from praktika.settings import Settings  # noqa: E402

ARCHIVES = {"mod": "go-mod-cache.tar.zst", "build": "go-build-cache.tar.zst"}
_ENV_VAR = {"mod": "GOMODCACHE", "build": "GOCACHE"}

# The mod cache is fully determined by the module set, i.e. the go.mod/go.sum
# files. We stamp the populated GOMODCACHE with a hash of these so a later
# restore can prove the dir already holds the right modules and skip re-extracting
# the (multi-hundred-MB) tarball -- the common case on a cortage, where members
# share one GOMODCACHE with no cleanup, and on any reused runner. The build cache
# is run-scoped / content-addressed, so it is not keyed and always restored.
_MOD_KEY_FILES = [
    "flow/go.mod",
    "flow/go.sum",
    "flow/pkg/go.mod",
    "flow/pkg/go.sum",
]
_SENTINEL = ".peerdb-cache-key"


def _cache_dir(kind):
    return subprocess.check_output(["go", "env", _ENV_VAR[kind]], text=True).strip()


def _mod_cache_key():
    """sha256 over the go.mod/go.sum files that define the module set (relative to
    CWD, the repo root for every CI job). Returns None if none are present -- then
    we can't prove the cache is current and must restore."""
    h = hashlib.sha256()
    found = False
    for rel in _MOD_KEY_FILES:
        try:
            with open(rel, "rb") as f:
                data = f.read()
        except OSError:
            continue
        found = True
        h.update(rel.encode())
        h.update(b"\0")
        h.update(data)
        h.update(b"\0")
    return h.hexdigest() if found else None


def _mark(kind, cache_dir):
    """Stamp `cache_dir` with the current mod key so a later restore can skip the
    untar. No-op for the build cache (run-scoped, not keyed)."""
    if kind != "mod":
        return
    key = _mod_cache_key()
    if not key:
        return
    try:
        with open(os.path.join(cache_dir, _SENTINEL), "w") as f:
            f.write(key)
    except OSError as e:
        print(f"WARNING: could not stamp {kind} cache sentinel: {e}", flush=True)


def _run(cmd):
    print(f"+ {cmd}", flush=True)
    return subprocess.run(cmd, shell=True).returncode


def pack(kind, out):
    """Tar the kind's cache dir into ``out`` (zstd). Returns a shell return code."""
    cache_dir = _cache_dir(kind)
    os.makedirs(cache_dir, exist_ok=True)
    # Keep the sentinel out of the artifact so it never ships a stale key to an
    # off-runner consumer; each consumer stamps its own dir on restore.
    rc = _run(f"tar -C {cache_dir} --exclude=./{_SENTINEL} -cf - . | zstd -T0 -3 > {out}")
    _run(f"ls -lh {out} || true")
    # Stamp the producer's own dir so a later consumer sharing this runner skips
    # the untar.
    if rc == 0:
        _mark(kind, cache_dir)
    return rc


def restore(kind):
    """Extract the kind's artifact (if present in INPUT_DIR) into its cache dir.
    Best effort: always returns 0 so a missing/broken cache never fails the job."""
    cache_dir = _cache_dir(kind)

    # Fast path: the dir is already populated for this exact module set (a cortage
    # predecessor or an earlier job on a reused runner left it, and cortage members
    # share the dir with no cleanup). Skip re-extracting the multi-hundred-MB tar.
    if kind == "mod":
        key = _mod_cache_key()
        sentinel = os.path.join(cache_dir, _SENTINEL)
        if key:
            try:
                already = open(sentinel).read().strip() == key
            except OSError:
                already = False
            if already:
                print(
                    f"{kind} cache already populated for this go.sum "
                    f"({cache_dir}); skipping restore",
                    flush=True,
                )
                return 0

    archive = os.path.join(Settings.INPUT_DIR, ARCHIVES[kind])
    if not os.path.exists(archive):
        print(f"no {kind} cache artifact ({archive}); job runs cold", flush=True)
        return 0
    os.makedirs(cache_dir, exist_ok=True)
    print(f"restoring {kind} cache into {cache_dir}", flush=True)
    if _run(f"zstd -d -T0 < {archive} | tar -x -C {cache_dir}") != 0:
        print(f"WARNING: failed to restore {kind} cache; job runs cold", flush=True)
        return 0
    # Stamp the restored dir so a subsequent restore on this runner skips the untar.
    _mark(kind, cache_dir)
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
