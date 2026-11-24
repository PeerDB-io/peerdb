#!/usr/bin/env python3
"""Attribute e2e test time to waits and report runner CPU use during the test phase.

Inputs come from one tilt-flow job: the JSONL wait records that flow/e2e/timings.go writes,
the gotestsum JUnit files, `vmstat -n -t` output, and timestamped `docker stats` lines.
Prints a Markdown report (for $GITHUB_STEP_SUMMARY) and writes the numbers as JSON.
"""
import argparse
import collections
import glob
import json
import math
import os
import statistics
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone


def leaf_tests(junit_paths):
    """Duration in seconds of each leaf test that ran, keyed by test name."""
    tests = {}
    for path in junit_paths:
        if not os.path.exists(path):
            continue
        cases = [c for c in ET.parse(path).getroot().iter("testcase") if c.find("skipped") is None]
        names = {c.get("name") for c in cases}
        for c in cases:
            name = c.get("name")
            # a parent test's time includes its subtests, so only leaves are summed
            if any(other.startswith(name + "/") for other in names):
                continue
            tests[name] = tests.get(name, 0.0) + float(c.get("time") or 0)
    return tests


def wait_records(timings_dir):
    records = []
    for path in glob.glob(os.path.join(timings_dir, "*.jsonl")):
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
    return records


def cpu_samples(vmstat_path, start, end):
    """Busy percent and run queue per vmstat sample inside [start, end]."""
    if not vmstat_path or not os.path.exists(vmstat_path):
        return []
    samples, header, first = [], None, True
    with open(vmstat_path, encoding="utf-8") as f:
        for line in f:
            fields = line.split()
            if "id" in fields and "r" in fields:
                header = fields
                continue
            if header is None or not fields or not fields[0].isdigit():
                continue
            if first:
                # the first row reports averages since boot, not the last interval
                first = False
                continue
            # -t appends the date and the time after the named columns
            values = dict(zip(header, fields))
            try:
                ts = datetime.strptime(" ".join(fields[-2:]), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp()
            except ValueError:
                continue
            if start <= ts <= end:
                samples.append({"busy": 100 - int(values["id"]), "r": int(values["r"]), "free_kb": int(values["free"])})
    return samples


def container_cpu(stats_path, start, end):
    """Mean and max CPU percent (100 = one core) per container inside [start, end]."""
    per = collections.defaultdict(list)
    if not stats_path or not os.path.exists(stats_path):
        return {}
    with open(stats_path, encoding="utf-8") as f:
        for line in f:
            ts, _, payload = line.partition(" ")
            try:
                if not start <= int(ts) <= end:
                    continue
                row = json.loads(payload)
                per[row["Name"]].append(float(row["CPUPerc"].rstrip("%")))
            except (ValueError, KeyError):
                continue
    return {name: {"mean": statistics.mean(v), "max": max(v)} for name, v in per.items()}


def pct(part, whole):
    return 100.0 * part / whole if whole else 0.0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--timings-dir", required=True)
    ap.add_argument("--junit", nargs="+", required=True)
    ap.add_argument("--vmstat")
    ap.add_argument("--docker-stats")
    ap.add_argument("--phase-file", help="file with the test phase start and end as Unix seconds, one per line")
    ap.add_argument("--job-start-file", help="file with the job start as Unix seconds")
    ap.add_argument("--out-json", required=True)
    ap.add_argument("--label", default="")
    args = ap.parse_args()

    tests = leaf_tests(args.junit)
    records = wait_records(args.timings_dir)
    by_test = collections.defaultdict(lambda: collections.Counter())
    for r in records:
        by_test[r["test"]][r["kind"]] += r["duration_ms"] / 1000.0

    test_seconds = sum(tests.values())
    instrumented = {name: t for name, t in tests.items() if name in by_test}
    instrumented_seconds = sum(instrumented.values())
    kinds = collections.Counter()
    for name in instrumented:
        kinds.update(by_test[name])

    phase = [0, float("inf")]
    if args.phase_file and os.path.exists(args.phase_file):
        with open(args.phase_file, encoding="utf-8") as f:
            values = [int(v) for v in f.read().split()]
        if len(values) >= 2:
            phase = values[:2]
    setup_seconds = None
    if args.job_start_file and os.path.exists(args.job_start_file) and phase[0]:
        with open(args.job_start_file, encoding="utf-8") as f:
            setup_seconds = phase[0] - int(f.read().split()[0])
    cpu = cpu_samples(args.vmstat, *phase)
    containers = container_cpu(args.docker_stats, *phase)

    result = {
        "label": args.label,
        "tests_ran": len(tests),
        "test_seconds": test_seconds,
        "instrumented_tests": len(instrumented),
        "instrumented_test_seconds": instrumented_seconds,
        "wait_seconds_by_kind": dict(kinds),
        "test_phase_seconds": phase[1] - phase[0] if phase[1] != float("inf") else None,
        "setup_seconds": setup_seconds,
        "cpu": {
            "samples": len(cpu),
            "busy_mean": statistics.mean(s["busy"] for s in cpu) if cpu else None,
            "busy_p95": sorted(s["busy"] for s in cpu)[math.ceil(0.95 * len(cpu)) - 1] if cpu else None,
            "runqueue_mean": statistics.mean(s["r"] for s in cpu) if cpu else None,
            "runqueue_max": max(s["r"] for s in cpu) if cpu else None,
            "free_kb_min": min(s["free_kb"] for s in cpu) if cpu else None,
            "cores": os.cpu_count(),
        },
        "containers": containers,
        "tests": {name: {"seconds": t, **by_test.get(name, {})} for name, t in tests.items()},
    }
    with open(args.out_json, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=1)

    out = sys.stdout
    out.write(f"### e2e time and CPU: {args.label}\n\n")
    if setup_seconds is not None:
        out.write(f"Setup: {setup_seconds} s from job start to the test phase. ")
    if result["test_phase_seconds"] is not None:
        out.write(f"Test phase: {result['test_phase_seconds']} s.\n\n")
    out.write(f"{len(tests)} tests ran, {test_seconds:.0f} test-seconds. ")
    out.write(f"{len(instrumented)} tests used the e2e wait helpers ({instrumented_seconds:.0f} test-seconds).\n\n")
    out.write("| Wait kind | Seconds | Share of instrumented test-seconds |\n|---|---|---|\n")
    for kind in ("status", "wait", "finish"):
        out.write(f"| {kind} | {kinds[kind]:.0f} | {pct(kinds[kind], instrumented_seconds):.0f}% |\n")
    rest = instrumented_seconds - sum(kinds.values())
    out.write(f"| outside waits | {rest:.0f} | {pct(rest, instrumented_seconds):.0f}% |\n\n")
    c = result["cpu"]
    if cpu:
        out.write(
            f"CPU during the test phase ({c['cores']} cores, {len(cpu)} samples): "
            f"busy mean {c['busy_mean']:.0f}%, p95 {c['busy_p95']}%, "
            f"run queue mean {c['runqueue_mean']:.1f}, max {c['runqueue_max']}, "
            f"min free memory {c['free_kb_min'] / 1048576:.1f} GiB.\n\n"
        )
    if containers:
        out.write("| Container | Mean CPU % | Max CPU % |\n|---|---|---|\n")
        for name, v in sorted(containers.items(), key=lambda kv: -kv[1]["mean"])[:10]:
            out.write(f"| {name} | {v['mean']:.0f} | {v['max']:.0f} |\n")
        out.write("\n")


if __name__ == "__main__":
    main()
