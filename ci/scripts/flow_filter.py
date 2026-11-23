#!/usr/bin/env python3
"""Workflow filter hook: per-setup skipping of the flow_test matrix.

Registered as `workflow_filter_hooks` on the PR workflow. Praktika calls
`should_skip_flow_test(job_name)` for every job and skips the ones it returns
True for. Hook skips run *before* the digest/changed-files filter and are final
(a hook-skipped job is removed entirely and cannot be rescued by another job's
`requires`), so this is what lets a flow_test permutation skip even though it
requires the shared flow-images / go-build-cache artifacts.

Decision for a `Flow test (<label>)` job, from the PR's changed files:
  - any change under a flow-relevant path that is NOT owned by some setup
    (core flow source, protos, the Tilt/compose stack, the drivers) -> run every
    perm (return False); such code is shared, so no perm is safe to skip;
  - else, if the change touches no owned path at all (e.g. docs/UI/nexus only) ->
    defer (return False) and let the digest filter skip the whole matrix;
  - else the changes are confined to owned setups: run this perm only if one of
    the changed files is under ITS owned prefixes, otherwise skip.

Fail open: if the changed-file list is unavailable, nothing is skipped. The
`ci-force-all` label bypasses this hook in praktika before it is called.
"""
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.getcwd())

from praktika.info import Info  # noqa: E402

import flow_matrix  # noqa: E402

# Paths whose change affects a flow_test run. The union of the flow-test job
# digest and its producers' digests (ci/workflows/pull_request.py); a changed
# file outside all of these is not flow-relevant to the matrix.
_FLOW_RELEVANT = [
    "flow",
    "protos",
    "buf.gen.yaml",
    "generate-protos.sh",
    "Tiltfile",
    "docker-compose-dev.yml",
    "ancillary-docker-compose.yml",
    "stacks",
    "ci/scripts/flow_test.py",
    "ci/scripts/tilt_env.py",
    "ci/scripts/flow_matrix.py",
    "ci/scripts/flow_images.py",
    "ci/scripts/go_build_cache.py",
    "ci/scripts/go_cache.py",
    "ci/scripts/go_mod_cache.py",
    ".github/scripts/select-flow-tests.sh",
]

_LABEL_RE = re.compile(r"^Flow test \((.+)\)$")


def _norm(path):
    return os.path.normpath(path.lstrip("./")) if path else path


def _under(prefixes, path):
    return any(path == p or path.startswith(p + "/") for p in prefixes)


def should_skip_flow_test(job_name):
    m = _LABEL_RE.match(job_name)
    if not m:
        return False, ""  # not a flow_test perm -- never our decision
    label = m.group(1)

    changed = Info().get_changed_files()
    if not changed:
        return False, ""  # unknown changes -> run (fail open)
    changed = [_norm(f) for f in changed]

    # A shared/core flow change (anything flow-relevant but not setup-owned)
    # could affect any perm -> run them all.
    if any(_under(_FLOW_RELEVANT, f) and not _under(flow_matrix.ALL_OWNED, f)
           for f in changed):
        return False, ""

    owned_hits = [f for f in changed if _under(flow_matrix.ALL_OWNED, f)]
    if not owned_hits:
        # No flow-owned change (e.g. docs/UI/nexus only); let the digest filter
        # decide whether the matrix runs at all.
        return False, ""

    mine = flow_matrix.owned_prefixes(label)
    if any(_under(mine, f) for f in owned_hits):
        return False, ""  # this setup was touched -> run
    return True, "flow changes confined to other setups"
