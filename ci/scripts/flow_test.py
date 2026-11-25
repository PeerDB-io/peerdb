#!/usr/bin/env python3
"""Flow e2e driver — port of flow.yml's `flow_test` matrix (one permutation).

Brings up the Tilt + docker-compose stack (ci/scripts/tilt_env.py), selects the
tilt-tagged tests, runs them under gotestsum, and emits a structured praktika
Result: one sub-result per module (flow, flow/pkg) whose rows are the individual
test cases (failures first), via praktika's native Result.from_junit_run. A
setup failure before the suite runs is reported as a single ERROR Result with
the Tilt startup log + docker diagnostics attached; a test failure attaches the
same diagnostics.

Driven entirely by environment (set per-permutation in the Job command):
  TEST_PACKAGES       flow/ test package patterns (required)
  PKG_TEST_PACKAGES   flow/pkg/ test package patterns (optional)
  ANCILLARY_SERVICES  Tilt ancillary resources to start (required)
  RUN_LABEL           short label for the report / logs
  TEST_ENV            newline-separated KEY=VALUE overrides for the test run
  <DB>_IMAGE          POSTGRES_IMAGE / CLICKHOUSE_IMAGE / ... ancillary pins

NOTE (increment 1): the AWS / RDS-IAM / CloudSQL / cloud-destination suites are
skipped (FLOW_TESTS_RDS_IAM_AUTH_SKIP defaults to true, no cloud creds). Secret
re-homing lands in a later phase.
"""
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile

# Make praktika + the sibling helpers importable from the repo root.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.getcwd())

from praktika.result import Result  # noqa: E402
from praktika.settings import Settings  # noqa: E402
from praktika.utils import Utils  # noqa: E402

import go_cache  # noqa: E402
import tilt_env  # noqa: E402

FLOW_IMAGES_ARCHIVE = "flow-images.tar.zst"

FLOW_JUNIT = "logs/test-results.xml"
PKG_JUNIT = "logs/pkg-test-results.xml"
COVERPKG_FLOW = "github.com/PeerDB-io/peerdb/flow/..."
COVERPKG_PKG = "github.com/PeerDB-io/peerdb/flow/pkg/..."

# Code coverage (-cover) is expensive (≈2x test-compile) and the matrix doesn't
# gate on coverage.out — it's a reporting artifact only. Off by default so the
# flow matrix (and the Go build cache it shares) compile uninstrumented and fast;
# a future, non-blocking coverage job can run this driver with FLOW_COVERAGE=1.
# Producer (go_build_cache.py), selection (select-flow-tests.sh) and the run below
# must all agree, so _build_env re-exports this to the selection subprocess.
COVERAGE = os.environ.get("FLOW_COVERAGE", "0") == "1"


def _run(cmd, **kwargs):
    print(f"+ {cmd if isinstance(cmd, str) else ' '.join(cmd)}", flush=True)
    return subprocess.run(cmd, **kwargs).returncode


def _ensure_hosts_entry():
    """Map host.docker.internal to loopback so the host-run tests can reach the
    docker-compose published ports (mirrors tilt-flow.yml)."""
    with open("/etc/hosts") as f:
        if "host.docker.internal" in f.read():
            return
    subprocess.run(
        "echo '127.0.0.1 host.docker.internal' | sudo tee -a /etc/hosts",
        shell=True,
        check=True,
        stdout=subprocess.DEVNULL,
    )


def _resolve_target_label():
    """The matrix target for this run. Prefer an explicit MATRIX_TARGET (manual
    / local runs); otherwise derive it from the parametrized job name, which
    praktika sets to "Flow test (<label>)"."""
    explicit = os.environ.get("MATRIX_TARGET")
    if explicit:
        return explicit
    try:
        from praktika.info import Info

        job_name = Info().job_name or ""
    except Exception:
        job_name = os.environ.get("JOB_NAME", "")
    m = re.search(r"\(([^()]*)\)\s*$", job_name)
    return m.group(1) if m else ""


def _apply_matrix_target():
    """Resolve this run's permutation env from flow_matrix and apply it (without
    clobbering anything already exported, so values can still be overridden)."""
    label = _resolve_target_label()
    if not label:
        return
    import flow_matrix

    if label not in flow_matrix.LABELS:
        print(f"WARNING: job variant {label!r} not in flow_matrix; "
              "falling back to explicit env", flush=True)
        return
    for key, value in flow_matrix.resolve(label).items():
        os.environ.setdefault(key, value)


def _load_prebuilt_flow_images(env):
    """If the flow_images job's artifact is present (ci/scripts/flow_images.py,
    required by this job), docker-load it and tell the Tiltfile to skip the
    per-job image builds. Best effort: on any failure Tilt builds them itself."""
    archive = os.path.join(Settings.INPUT_DIR, FLOW_IMAGES_ARCHIVE)
    if not os.path.exists(archive):
        print("no prebuilt flow images artifact; Tilt will build them", flush=True)
        return
    print(f"loading prebuilt flow images from {archive}...", flush=True)
    if _run(f"zstd -d -T0 < {archive} | docker load", shell=True) == 0:
        env["PEERDB_CI_PREBUILT_FLOW_IMAGES"] = "1"
        print("prebuilt flow images loaded; Tilt will skip the image builds", flush=True)
    else:
        print("WARNING: failed to load prebuilt flow images; Tilt will build them", flush=True)


def _build_env():
    """Static env mirroring tilt-flow.yml's top-level `env:` block."""
    env = os.environ.copy()
    services = env.get("ANCILLARY_SERVICES", "")
    env.update(
        {
            "PEERDB_CI": "1",
            "TILT_PORT": tilt_env.TILT_PORT,
            "DO_NOT_TRACK": "1",
            "TZ": "UTC",
            "CORE_SERVICE_RECOVERY_ATTEMPTS": env.get("CORE_SERVICE_RECOVERY_ATTEMPTS", "3"),
            "MIN_RECOVERY_TIME": env.get("MIN_RECOVERY_TIME", "3"),
            "MAX_RECOVERY_TIME": env.get("MAX_RECOVERY_TIME", "5"),
            # Enable OTEL in PeerDB only if the collector is among the services.
            "ENABLE_OTEL_METRICS": "true" if "otel-collector" in services.split() else "false",
            # Skip the AWS/RDS-IAM-coupled tests until their secrets are re-homed.
            "FLOW_TESTS_RDS_IAM_AUTH_SKIP": env.get("FLOW_TESTS_RDS_IAM_AUTH_SKIP", "true"),
            # Pin coverage for the selection subprocess (select-flow-tests.sh) to
            # our decision, so its -list compile matches the run's cache keys. The
            # shared script defaults to on (GHA); here we make it explicit.
            "FLOW_COVERAGE": "1" if COVERAGE else "0",
        }
    )
    return env


def _selected_command(label, junit_abs, timeout, coverpkg, coverdir, selectiondir, parallel):
    """Port of tilt-flow.yml's run_selected_tests: read the package/test pairs
    chosen by select-flow-tests.sh and build the gotestsum command that runs
    exactly those. Returns a shell command string, or None when the selection
    was empty. (A string, not an argv list: from_junit_run treats a list as
    several separate commands, so the single gotestsum invocation is joined.)"""
    tsv = os.path.join(selectiondir, f"{label}-selected.tsv")
    names, pkgs, prev = [], [], None
    with open(tsv) as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            package, _, test_name = line.partition("\t")
            names.append(test_name)
            if package != prev:
                pkgs.append(package)
                prev = package
    if not pkgs:
        print(f"{label} selection: 0 packages", flush=True)
        return None
    uniq = sorted(set(names))
    regex = "^(" + "|".join(uniq) + ")$"
    print(f"{label} selection: {len(pkgs)} packages, {len(uniq)} tests", flush=True)
    cover_build = ["-cover", "-coverpkg", coverpkg] if COVERAGE else []
    cover_run = ["-args", f"-test.gocoverdir={coverdir}"] if COVERAGE else []
    cmd = [
        "gotestsum", "--format", "standard-quiet", "--no-color",
        "--junitfile", junit_abs, "--",
        "-tags", "tilt", *cover_build,
        "-p", "32", "-parallel", str(parallel),
        *pkgs, "-run", regex, "-timeout", timeout,
        *cover_run,
    ]
    return shlex.join(cmd)


def _module_result(cmd, junit_abs, name, module_dir, env, empty_status):
    """Run the module's selected tests via from_junit_run, or short-circuit to
    `empty_status` with no command (empty selection wrote no JUnit report)."""
    if cmd is None:
        return Result.create_from(
            name=name, status=empty_status, info="no tilt-tagged tests selected",
        )
    return Result.from_junit_run(
        command=cmd, report_glob=junit_abs, name=name, workdir=module_dir, env=env,
    )


def main():
    sw = Utils.Stopwatch()
    _apply_matrix_target()
    label = os.environ.get("RUN_LABEL") or "flow"
    runner_temp = os.environ.get("RUNNER_TEMP") or tempfile.mkdtemp()
    os.environ["RUNNER_TEMP"] = runner_temp
    os.makedirs(runner_temp, exist_ok=True)
    os.makedirs("logs", exist_ok=True)

    env = _build_env()
    services = env.get("ANCILLARY_SERVICES", "").split()
    test_packages = env.get("TEST_PACKAGES", "").strip()
    pkg_packages = env.get("PKG_TEST_PACKAGES", "").strip()

    if not test_packages or not services:
        Result.create_from(
            status=Result.Status.ERROR, stopwatch=sw,
            info="TEST_PACKAGES and ANCILLARY_SERVICES are required",
        ).complete_job()
        return

    print(f"=== flow_test: {label} ===", flush=True)
    print(f"    services:      {' '.join(services)}", flush=True)
    print(f"    test_packages: {test_packages}", flush=True)
    print(f"    pkg_packages:  {pkg_packages}", flush=True)

    # ---- Setup: Tilt + docker-compose stack ---------------------------------
    tilt_log = None
    try:
        # Reused runners may carry a previous job's tilt/compose stack (GitHub's
        # are ephemeral); clear it so `tilt up` can bind the control-API port.
        tilt_env.reset_stale_state()
        if not os.path.exists(".env"):
            shutil.copyfile(".env.example", ".env")
        # Disposable CI catalog Postgres (Temporal's persistence): disable fsync
        # so Temporal commit latency isn't bound by network-attached storage.
        # docker-compose-dev.yml interpolates this into the catalog command.
        # MUST be quoted: the provision scripts `source` .env with `set -e`, and
        # an unquoted multi-word value parses as a bogus command (exit 127),
        # breaking every provision/peer-setup step. Compose strips the quotes.
        with open(".env", "a") as f:
            f.write('\nCATALOG_EXTRA_PG_FLAGS="-c fsync=off -c synchronous_commit=off -c full_page_writes=off"\n')
        _ensure_hosts_entry()
        _load_prebuilt_flow_images(env)
        # Restore the shared Go caches (modules + compiled objects) so the test
        # compile reuses them instead of re-downloading / rebuilding from scratch.
        go_cache.restore("mod")
        go_cache.restore("build")
        if _run("go mod download", shell=True, cwd="flow", env=env) != 0:
            raise tilt_env.TiltError("go mod download failed")
        # Point at the log start_tilt writes *before* calling it, so a start_tilt
        # failure (e.g. Tiltfile eval timeout) still attaches the startup log.
        tilt_log = os.path.join(runner_temp, "tilt-startup.log")
        tilt_env.start_tilt(runner_temp, env)
        tilt_env.enable_and_trigger(services)
        tilt_env.wait_ancillary(services)
        tilt_env.bring_up_core(
            attempts=int(env["CORE_SERVICE_RECOVERY_ATTEMPTS"]),
            min_t=int(env["MIN_RECOVERY_TIME"]),
            max_t=int(env["MAX_RECOVERY_TIME"]),
        )
    except Exception as e:
        diag = tilt_env.capture_diagnostics("logs")
        files = ([tilt_log] if tilt_log and os.path.exists(tilt_log) else []) + diag
        Result.create_from(
            status=Result.Status.ERROR, stopwatch=sw,
            info=f"flow setup failed: {e}", files=files,
        ).complete_job()
        return

    # ---- Select tilt-tagged tests -------------------------------------------
    if _run(["bash", ".github/scripts/select-flow-tests.sh"], env=env) != 0:
        diag = tilt_env.capture_diagnostics("logs")
        Result.create_from(
            status=Result.Status.ERROR, stopwatch=sw,
            info="test selection failed", files=([tilt_log] if tilt_log else []) + diag,
        ).complete_job()
        return

    # ---- Run selected tests on the host against Tilt ------------------------
    # Apply per-run TEST_ENV overrides (take precedence over the tests' .env
    # autoload, matching godotenv semantics).
    for override in env.get("TEST_ENV", "").splitlines():
        override = override.strip()
        if override and "=" in override:
            k, _, v = override.partition("=")
            print(f"test env override: {override}", flush=True)
            env[k] = v

    coverdir = os.path.join(runner_temp, "gocoverdir")
    selectiondir = os.path.join(runner_temp, "test-selection")
    if COVERAGE:
        os.makedirs(coverdir, exist_ok=True)
    parallel = env.get("TEST_PARALLEL", "64")
    flow_junit_abs = os.path.abspath(FLOW_JUNIT)
    pkg_junit_abs = os.path.abspath(PKG_JUNIT)

    flow_cmd = _selected_command(
        "flow", flow_junit_abs, "1200s", COVERPKG_FLOW, coverdir, selectiondir, parallel,
    )
    # An empty flow selection is unexpected (ERROR); an empty pkg selection is a
    # normal SKIP, matching tilt-flow.yml.
    flow_res = _module_result(
        flow_cmd, flow_junit_abs, f"flow e2e ({label})", "flow", env, Result.Status.ERROR,
    )

    pkg_res = None
    if pkg_packages:
        pkg_cmd = _selected_command(
            "pkg", pkg_junit_abs, "300s", COVERPKG_PKG, coverdir, selectiondir, parallel,
        )
        pkg_res = _module_result(
            pkg_cmd, pkg_junit_abs, f"flow/pkg e2e ({label})", "flow/pkg", env,
            Result.Status.SKIPPED,
        )

    # Emit coverage even on failure so the attached artifact always has data.
    # Skipped when coverage is off (the matrix default): no gocoverdir was written.
    if COVERAGE:
        subprocess.run(
            ["go", "tool", "covdata", "textfmt", f"-i={coverdir}", "-o", "coverage.out"],
            check=False,
        )

    results = [flow_res]
    if pkg_res is not None:
        results.append(pkg_res)

    # Diagnostics + coverage go on the TOP-level Result: praktika only uploads
    # files attached to the job's root result, not nested sub-results.
    top_files = []
    if os.path.exists("coverage.out"):
        top_files.append("coverage.out")
    if not flow_res.is_ok() or (pkg_res is not None and not pkg_res.is_ok()):
        if tilt_log and os.path.exists(tilt_log):
            top_files.append(tilt_log)
        top_files += tilt_env.capture_diagnostics("logs")

    Result.create_from(results=results, stopwatch=sw, files=top_files).complete_job()


if __name__ == "__main__":
    main()
