#!/usr/bin/env python3
"""Port of ci.yml's catalog-migration check (the non-cloud half).

ci.yml runs, per Postgres 14-18: cargo check + `go run . migrate` + cargo test.
`cargo check` is covered by the Lint job's nexus clippy; `cargo test` needs
GCP/Snowflake creds and is deferred with the rest of the cloud work. What's
runnable now is verifying the catalog migrations apply cleanly on each Postgres
major version — a Go + Postgres check, no creds.

The Postgres version comes from the parametrized job name "Catalog migrate
(pg-NN)" (or an explicit PG_VERSION). A disposable postgres:<ver>-alpine
container is started for the run and torn down after.
"""
import os
import re
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.getcwd())

from praktika.info import Info  # noqa: E402
from praktika.result import Result  # noqa: E402
from praktika.utils import Utils  # noqa: E402

CONTAINER = "catalog_peer"
PORT = "7132"


def _run(cmd, **kwargs):
    print(f"+ {cmd}", flush=True)
    return subprocess.run(cmd, shell=True, **kwargs).returncode


def main():
    sw = Utils.Stopwatch()
    # The parametrized job name ("Catalog migrate (pg-NN)") carries the Postgres
    # version; PG_VERSION overrides it. The job name also names the Result below.
    job_name = Info().job_name or ""
    m = re.search(r"pg-(\d+)", job_name)
    pg = os.environ.get("PG_VERSION") or (m.group(1) if m else "18")
    env = os.environ.copy()
    env.update({
        "PEERDB_CATALOG_HOST": "localhost",
        "PEERDB_CATALOG_PORT": PORT,
        "PEERDB_CATALOG_DATABASE": "postgres",
        "PEERDB_CATALOG_USER": "postgres",
        "PEERDB_CATALOG_PASSWORD": "postgres",
    })

    status, info = Result.Status.OK, ""
    subprocess.run(f"docker rm -f {CONTAINER}", shell=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        if _run(
            f"docker run -d --name {CONTAINER} -p {PORT}:5432 "
            "-e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres "
            f"-e POSTGRES_DB=postgres postgres:{pg}-alpine"
        ) != 0:
            status, info = Result.Status.ERROR, f"failed to start postgres:{pg}-alpine"
        else:
            ready = False
            for _ in range(60):
                if subprocess.run(
                    f"docker exec {CONTAINER} pg_isready -U postgres",
                    shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                ).returncode == 0:
                    ready = True
                    break
                time.sleep(1)
            if not ready:
                status, info = Result.Status.ERROR, "postgres did not become ready"
            elif _run("go run . migrate", cwd="flow", env=env) != 0:
                status = Result.Status.FAIL
    except Exception as e:  # noqa: BLE001
        status, info = Result.Status.ERROR, str(e)
    finally:
        subprocess.run(f"docker rm -f {CONTAINER}", shell=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    # Name the Result exactly the registered job name — the workflow report keys
    # rows by job name, so a hand-written variant never updates its row (stuck
    # RUNNING while the check goes green).
    Result.create_from(
        name=job_name, status=status, stopwatch=sw, info=info
    ).complete_job()


if __name__ == "__main__":
    main()
