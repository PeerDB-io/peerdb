#!/usr/bin/env python3
"""Shared Go cache pack/restore for the PeerDB Praktika CI.

Two run-shared Go caches, delivered differently:

  mod   -> GOMODCACHE (`go env GOMODCACHE`): downloaded module source. A
           self-healing, content-addressed S3 cache keyed on the go.mod/go.sum
           set -- the Praktika analogue of GHA setup-go's go.sum-keyed module
           cache. There is NO producer job: every Go job's `restore mod` pre-hook
           downloads the bundle if present, else runs `go mod download` cold, and
           its `save mod` post-hook packs + uploads the bundle when it was built
           cold and no one has published it yet. So the module cache is never a
           node the critical path waits on: on a hit a job just unpacks the
           tarball (as before); on a miss (deps moved) the first Go job folds the
           download into runtime it was already spending and publishes for the
           rest. GOMODCACHE is downloaded module source -- arch/OS independent --
           so one bundle serves every consumer across all PRs with the same
           go.sum.

  build -> GOCACHE (`go env GOCACHE`): compiled package/test objects. Run-scoped
           (any .go edit changes it), so it is rebuilt each run by the Go build
           cache job and shipped as a Praktika S3 artifact that the flow-test
           matrix restores from INPUT_DIR.

restore() extracts into the consumer's own `go env` locations (the defaults), so
no `go env -w` is needed and differing $HOME between producer and consumer does
not matter. restore() is best-effort: a missing/corrupt cache just means the job
does the work cold.

restore("mod") skips the untar when the GOMODCACHE is already populated for the
current go.mod/go.sum (stamped by a prior pack/restore on the same runner) -- the
common case on a cortage, where members share one GOMODCACHE with no cleanup, so
re-extracting the ~800 MB tarball on every member is pure waste.

CLI (for use as a job pre_hook / post_hook / command):
    go_cache.py restore mod|build
    go_cache.py save    mod
    go_cache.py pack    mod|build <out.tar.zst>
"""
import hashlib
import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.getcwd())

from praktika.settings import Settings  # noqa: E402
from praktika.s3 import S3  # noqa: E402

ARCHIVES = {"mod": "go-mod-cache.tar.zst", "build": "go-build-cache.tar.zst"}
_ENV_VAR = {"mod": "GOMODCACHE", "build": "GOCACHE"}

# The Go modules the CI builds live in two modules: flow and flow/pkg.
_GO_MODULE_DIRS = ["flow", "flow/pkg"]

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

# A job that restored the mod cache cold (S3 miss) drops this marker so its
# `save mod` post-hook knows it owes an upload. Lives in TEMP_DIR (ci/tmp), which
# the pre-hook and post-hook of a job share. The file holds the mod key it was
# built for, so a save validates the dir still matches before publishing.
_DIRTY_MARKER = os.path.join(Settings.TEMP_DIR, ".peerdb-mod-cache-dirty")


def _cache_dir(kind):
    return subprocess.check_output(["go", "env", _ENV_VAR[kind]], text=True).strip()


def _mod_s3_path(key):
    """Content-addressed S3 location of the GOMODCACHE bundle for a mod key. Shares
    the submodule cache's bucket/prefix (runners already allow it)."""
    return f"{Settings.CACHE_S3_PATH}/go-mod/{key}.tar.zst"


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


def _go_mod_download():
    """Populate GOMODCACHE cold, for both modules. Returns a shell return code."""
    rc = 0
    for d in _GO_MODULE_DIRS:
        print(f"+ (cd {d} && go mod download)", flush=True)
        rc = subprocess.run("go mod download", shell=True, cwd=d).returncode or rc
    return rc


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


def _restore_build():
    """Extract the Go build cache S3 artifact (if present in INPUT_DIR) into
    GOCACHE. Best effort: always returns 0 so a missing/broken cache never fails
    the job."""
    cache_dir = _cache_dir("build")
    archive = os.path.join(Settings.INPUT_DIR, ARCHIVES["build"])
    if not os.path.exists(archive):
        print(f"no build cache artifact ({archive}); job runs cold", flush=True)
        return 0
    os.makedirs(cache_dir, exist_ok=True)
    print(f"restoring build cache into {cache_dir}", flush=True)
    if _run(f"zstd -d -T0 < {archive} | tar -x -C {cache_dir}") != 0:
        print("WARNING: failed to restore build cache; job runs cold", flush=True)
    return 0


def _restore_mod():
    """Restore GOMODCACHE from the content-addressed S3 cache, else populate it
    cold with `go mod download` and mark the dir for a later `save`. Best effort:
    always returns 0."""
    cache_dir = _cache_dir("mod")
    key = _mod_cache_key()
    if not key:
        print("no go.mod/go.sum found; skipping mod cache restore", flush=True)
        return 0

    # Fast path: the dir is already populated for this exact module set (a cortage
    # predecessor or an earlier job on a reused runner left it, and cortage members
    # share the dir with no cleanup). Skip re-extracting the multi-hundred-MB tar.
    sentinel = os.path.join(cache_dir, _SENTINEL)
    try:
        already = open(sentinel).read().strip() == key
    except OSError:
        already = False
    if already:
        print(
            f"mod cache already populated for this go.sum ({cache_dir}); "
            "skipping restore",
            flush=True,
        )
        return 0

    # S3 hit: multipart download + untar. copy_file_from_s3 returns False on a
    # 404 (no_strict), so a miss just falls through to the cold path below.
    s3_path = _mod_s3_path(key)
    archive = os.path.join(Settings.TEMP_DIR, ARCHIVES["mod"])
    os.makedirs(Settings.TEMP_DIR, exist_ok=True)
    if S3.copy_file_from_s3(s3_path=s3_path, local_path=archive, no_strict=True) and (
        os.path.exists(archive)
    ):
        os.makedirs(cache_dir, exist_ok=True)
        print(f"mod cache hit {s3_path}; restoring into {cache_dir}", flush=True)
        if _run(f"zstd -d -T0 < {archive} | tar -x -C {cache_dir}") == 0:
            _run(f"rm -f {archive}")
            _mark("mod", cache_dir)
            return 0
        print("WARNING: failed to extract mod cache; populating cold", flush=True)
        _run(f"rm -f {archive}")

    # Cold path: download the modules ourselves (GHA setup-go's cache-miss
    # behavior). Only mark the dir for publishing if the download fully succeeded --
    # publishing a partial GOMODCACHE under the content-addressed key would poison
    # every future run (they'd restore it and skip the download). So on a failed
    # download we leave the job to surface the error and let a later clean run
    # populate the cache. GHA setup-go likewise only saves the cache on clean setup.
    print(f"mod cache miss {s3_path}; running `go mod download`", flush=True)
    if _go_mod_download() == 0:
        _mark("mod", cache_dir)
        try:
            with open(_DIRTY_MARKER, "w") as f:
                f.write(key)
        except OSError as e:
            print(f"WARNING: could not write mod-cache dirty marker: {e}", flush=True)
    else:
        print("WARNING: `go mod download` failed; not publishing mod cache", flush=True)
    return 0


def restore(kind):
    """Restore the kind's cache. Best effort: always returns 0 so a missing/broken
    cache never fails the job."""
    return _restore_mod() if kind == "mod" else _restore_build()


def save(kind):
    """Publish the GOMODCACHE bundle to the content-addressed S3 cache, but only if
    this runner built it cold (dirty marker present) and no one has published it
    yet. The post-hook side of the self-healing mod cache. Best effort: always
    returns 0 so an upload hiccup never reddens the job.

    Only `mod` is published this way; the build cache ships as a Praktika artifact
    from its producer job.
    """
    if kind != "mod":
        return 0
    if not os.path.exists(_DIRTY_MARKER):
        # Cache hit (or no Go work): nothing to publish.
        return 0

    try:
        key = _mod_cache_key()
        if not key:
            return 0
        s3_path = _mod_s3_path(key)

        # Someone (a sibling job, or a prior run) already published while we built.
        # The key is content-addressed, so their bundle is ours -- skip the upload.
        if S3.head_object(s3_path):
            print(f"mod cache already published {s3_path}; skipping upload", flush=True)
            return 0

        cache_dir = _cache_dir("mod")
        archive = os.path.join(Settings.TEMP_DIR, f"go-mod-{key}.tar.zst")
        if pack("mod", archive) != 0:
            print("WARNING: failed to pack mod cache; not publishing", flush=True)
            return 0
        # Multipart upload (boto3 upload_file). The object is content-addressed, so
        # concurrent uploaders write identical bytes and CompleteMultipartUpload is
        # atomic -- a reader never sees a partial object. The sole residual race (a
        # second uploader replacing the key mid-download, aborting the reader's
        # ranged GET) degrades to that reader's restore self-healing via `go mod
        # download`, so an unconditional multipart put is safe here and faster than
        # the single-part write-once PUT the (small) submodule cache uses.
        print(f"publishing mod cache -> {s3_path}", flush=True)
        S3.copy_file_to_s3(s3_path=s3_path, local_path=archive, with_rename=True,
                           no_strict=True)
        _run(f"rm -f {archive}")
    except Exception as e:
        print(f"WARNING: mod cache publish failed: {e}", flush=True)
    finally:
        try:
            os.remove(_DIRTY_MARKER)
        except OSError:
            pass
    return 0


def main(argv):
    if len(argv) == 2 and argv[0] == "restore" and argv[1] in ARCHIVES:
        return restore(argv[1])
    if len(argv) == 2 and argv[0] == "save" and argv[1] == "mod":
        return save(argv[1])
    if len(argv) == 3 and argv[0] == "pack" and argv[1] in ARCHIVES:
        return pack(argv[1], argv[2])
    raise SystemExit(
        "usage: go_cache.py restore mod|build | save mod | "
        "pack mod|build <out.tar.zst>"
    )


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
