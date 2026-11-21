#!/usr/bin/env python3
"""Tilt control-API helpers for the flow e2e driver (ci/scripts/flow_test.py).

PeerDB runs its e2e stack with Tilt driving docker-compose, so the job runs
directly on the runner (never inside a container). This module is the Python
port of the Tilt orchestration that used to live in tilt-flow.yml's bash:
start `tilt up` headless, enable/trigger/wait the ancillary services, and bring
up the core PeerDB services with per-resource recovery. Diagnostics capture on
failure lives here too.

Mirrors the shape of clickhouse-operator's ci/jobs/kind_env.py.
"""
import json
import os
import random
import subprocess
import time

TILT_PORT = os.environ.get("TILT_PORT", "10352")

# Core PeerDB services that must be healthy before tests run. proto-gen feeds
# the image builds; flow-migrate applies the catalog schema; the API/workers
# back the suites; minio backs S3 staging.
MAIN_SERVICES = [
    "proto-gen",
    "catalog",
    "flow-migrate",
    "flow-api",
    "flow-worker",
    "flow-snapshot-worker",
    "minio",
]


class TiltError(RuntimeError):
    """A setup/provisioning failure before the test suite runs."""


def _tilt(args, **kwargs):
    return subprocess.run(["tilt", "--port", TILT_PORT, *args], **kwargs)


def _resource_exists(res):
    return _tilt(
        ["get", "uiresource", res],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    ).returncode == 0


def start_tilt(runner_temp, env):
    """Launch `tilt up` headless (writing to a log file) and wait for the Tiltfile
    to finish evaluating. Returns (Popen, log_path)."""
    log_path = os.path.join(runner_temp, "tilt-startup.log")
    log = open(log_path, "wb")
    proc = subprocess.Popen(
        ["tilt", "up", "--port", TILT_PORT, "--stream=true", "--log-source=all"],
        stdout=log,
        stderr=subprocess.STDOUT,
        env=env,
    )

    print("Waiting for the Tilt API to come up...", flush=True)
    for _ in range(60):
        if _resource_exists("(Tiltfile)"):
            print("Tilt API is reachable.", flush=True)
            break
        if proc.poll() is not None:
            raise TiltError(f"tilt up exited early with code {proc.returncode}")
        time.sleep(2)
    else:
        raise TiltError("Tilt API did not become reachable")

    print("Waiting for the Tiltfile evaluation to complete...", flush=True)
    if _tilt(
        ["wait", "--for=condition=Ready", "uiresource/(Tiltfile)", "--timeout=300s"]
    ).returncode != 0:
        raise TiltError("Tiltfile evaluation did not complete")
    return proc, log_path


def enable_and_trigger(services):
    """Enable the requested ancillary resources and trigger each to start."""
    print(f"Enabling ancillary services: {' '.join(services)}", flush=True)
    if _tilt(["enable", *services]).returncode != 0:
        raise TiltError(f"failed to enable services: {' '.join(services)}")
    for svc in services:
        print(f"Triggering {svc}", flush=True)
        _tilt(["trigger", svc])


def _wait_ready(res, timeout="600s"):
    print(f"Waiting for {res} to be Ready...", flush=True)
    if _tilt(
        ["wait", "--for=condition=Ready", f"uiresource/{res}", f"--timeout={timeout}"]
    ).returncode != 0:
        _tilt(["logs", "--tail", "200", res])
        raise TiltError(f"{res} did not become Ready")


def wait_ancillary(services):
    """Wait for each ancillary service and its autostarted provisioning step.
    The 2-node ClickHouse cluster has an extra provisioning resource gated on
    node 2 + keeper; wait for it too so cluster tests can't race it."""
    for svc in services:
        _wait_ready(svc)
        if _resource_exists(f"provision-{svc}"):
            _wait_ready(f"provision-{svc}")
        if svc == "clickhouse-02":
            _wait_ready("provision-clickhouse-cluster")
    print("All requested ancillary services are up and running.", flush=True)


def _ready_status(res):
    """Classify a resource as 'ready', 'error', or 'pending'."""
    r = _tilt(
        ["get", "uiresource", res, "-o", "json"],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    if r.returncode != 0:
        return "pending"
    try:
        st = json.loads(r.stdout).get("status", {})
    except (ValueError, AttributeError):
        return "pending"
    upd = st.get("updateStatus", "none")
    run = st.get("runtimeStatus", "none")
    # A failed build/update or a crashed runtime means this attempt failed.
    if upd == "error" or run == "error":
        return "error"
    # Ready: update succeeded and the runtime is healthy (ok) or the resource
    # has no long-running process (not_applicable, e.g. proto-gen). A
    # missing/empty runtime status means it hasn't come up yet -> pending.
    if upd == "ok" and run in ("ok", "not_applicable"):
        return "ready"
    return "pending"


def _wait_or_recover(res, attempts, min_t, max_t, per_attempt):
    """Wait for a resource to become ready; on error or per-attempt timeout,
    reload it (`tilt trigger`) and retry, up to `attempts` times."""
    span = max(max_t - min_t + 1, 1)
    for attempt in range(1, attempts + 1):
        print(
            f"[{res}] readiness attempt {attempt}/{attempts} "
            f"(timeout {per_attempt}s)",
            flush=True,
        )
        deadline = time.monotonic() + per_attempt
        st = "pending"
        while time.monotonic() < deadline:
            st = _ready_status(res)
            if st == "ready":
                print(f"[{res}] is Ready.", flush=True)
                return True
            if st == "error":
                print(f"WARNING: [{res}] entered an error state.", flush=True)
                break
            time.sleep(min_t + random.randint(0, span - 1))
        # Don't reload after the final attempt (nothing left to wait on).
        if attempt < attempts:
            print(
                f"WARNING: [{res}] not healthy (status: {st}); reloading.",
                flush=True,
            )
            _tilt(["logs", "--tail", "100", res])
            _tilt(["trigger", res])
    print(
        f"ERROR: [{res}] failed to become Ready after {attempts} attempts.",
        flush=True,
    )
    _tilt(["logs", "--tail", "200", res])
    return False


def bring_up_core(services=None, attempts=3, min_t=3, max_t=5, per_attempt=600):
    """Bring up the core PeerDB services with recovery. Core services
    occasionally come up unhealthy on a cold start; reloading the individual
    resource recovers them."""
    for res in services or MAIN_SERVICES:
        if not _wait_or_recover(res, attempts, min_t, max_t, per_attempt):
            raise TiltError(
                f"Required core service {res} could not be recovered"
            )
    print("Core PeerDB services are up.", flush=True)


def capture_diagnostics(logs_dir="logs"):
    """Dump Tilt resource state + every container's logs into `logs_dir`.
    `tilt logs` is unavailable in this headless run (no web-UI websocket), so
    read the logs straight from Docker. Returns the list of files written."""
    os.makedirs(logs_dir, exist_ok=True)
    written = []
    try:
        r = _tilt(
            ["get", "uiresource", "-o", "json"],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        if r.returncode == 0:
            path = os.path.join(logs_dir, "tilt-resources.json")
            with open(path, "w") as f:
                f.write(r.stdout)
            written.append(path)
    except Exception as e:
        print(f"WARNING: failed to capture tilt resource state: {e}", flush=True)

    try:
        ps = subprocess.run(
            ["docker", "ps", "-a", "--format", "{{.Names}}\t{{.Status}}"],
            stdout=subprocess.PIPE,
            text=True,
        )
        ps_path = os.path.join(logs_dir, "docker-ps.txt")
        with open(ps_path, "w") as f:
            f.write(ps.stdout)
        written.append(ps_path)

        names = subprocess.run(
            ["docker", "ps", "-a", "--format", "{{.Names}}"],
            stdout=subprocess.PIPE,
            text=True,
        ).stdout.split()
        for c in names:
            log_path = os.path.join(logs_dir, f"docker-{c}.log")
            with open(log_path, "wb") as f:
                subprocess.run(
                    ["docker", "logs", "--timestamps", c],
                    stdout=f,
                    stderr=subprocess.STDOUT,
                )
            written.append(log_path)
    except Exception as e:
        print(f"WARNING: failed to capture docker logs: {e}", flush=True)
    return written
