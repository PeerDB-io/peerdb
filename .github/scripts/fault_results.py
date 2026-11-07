#!/usr/bin/env python3
"""Reads the JUnit files fault-run.sh writes.

  fault_results.py failed FILE           failed leaf tests in FILE, one per line
  fault_results.py rerun-pattern FILE    go test -run pattern rerunning them
  fault_results.py comment DIR           PR comment markdown for DIR, which holds
                                         one fault-results-<shard> directory per job
  fault_results.py failing-cases DIR     tests in DIR that failed on every attempt,
                                         sorted, for comparing code versions

comment reads RUN_URL (link to the workflow run) and EXPECTED_RESULTS
(space-separated <shard> names that should have uploaded results) from the
environment, and scenario names from flow/e2e/fault_known.go.
"""

import html
import os
import re
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path

MARKER = "<!-- fault-injection-results -->"
# GitHub rejects comments over 65,536 characters; leave room for the footer.
ATTEMPT_FILE = re.compile(r"attempt(\d+)-(.+)\.xml$")
QUOTA = re.compile(r"quota|rateLimitExceeded", re.IGNORECASE)


def cases(path):
    """Yields (name, outcome, text) for each leaf test case in a JUnit file."""
    entries = []
    for tc in ET.parse(path).getroot().iter("testcase"):
        name = tc.get("name", "")
        failure = tc.find("failure")
        if failure is None:
            failure = tc.find("error")
        if failure is not None:
            outcome, text = "fail", failure.text or failure.get("message", "")
        elif tc.find("skipped") is not None:
            outcome, text = "skip", tc.find("skipped").get("message", "")
        else:
            outcome, text = "pass", ""
        entries.append((name, outcome, text))
    names = {name for name, _, _ in entries}
    for name, outcome, text in entries:
        if not any(other.startswith(name + "/") for other in names):
            yield name, outcome, text


def failed(path):
    return [name for name, outcome, _ in cases(path) if outcome == "fail" and name.startswith("Test")]


def rerun_pattern(path):
    """Builds a -run pattern matching exactly the failed tests in path.

    go test matches each "/"-separated element against one level of test names,
    so the pattern is a per-level alternation. Tests in one package share every
    level but the last, so this selects no extra tests.
    """
    names = failed(path)
    if not names:
        return ""
    depth = max(name.count("/") for name in names) + 1
    levels = []
    for i in range(depth):
        parts = sorted({name.split("/")[i] for name in names if name.count("/") >= i})
        levels.append("^(" + "|".join(re.escape(p) for p in parts) + ")$")
    return "/".join(levels)


def collect(root):
    """Maps (package, test) to its attempts in order, as (outcome, text)."""
    runs = defaultdict(list)
    found = set()
    for shard_dir in sorted(Path(root).glob("fault-results-*")):
        found.add(shard_dir.name.removeprefix("fault-results-"))
        files = []
        for f in shard_dir.glob("attempt*.xml"):
            m = ATTEMPT_FILE.search(f.name)
            if m:
                files.append((int(m.group(1)), m.group(2), f))
        for _, package, f in sorted(files):
            try:
                for name, outcome, text in cases(f):
                    runs[(package, name)].append((outcome, text))
            except ET.ParseError as e:
                runs[(package, f"<unreadable {shard_dir.name}/{f.name}>")].append(("fail", str(e)))
    return runs, found


KNOWN_FAILURES = Path(__file__).resolve().parents[2] / "flow" / "e2e" / "fault_known.go"
GITHUB_COMMENT_LIMIT = 65_536
VIOLATION = re.compile(r"fault_harness\.go:\d+: (\[[0-9,]+\] .*?)(?: \(seen \d+ times.*)?$")
NOT_REACHED_SITES = re.compile(r"NOT REACHED: (.*) never fired")
PACKAGE_WORKLOAD = {
    "postgres_clickhouse": "pg_to_ch",
    "postgres_postgres": "pg_to_pg",
    "bigquery_clickhouse": "bq_to_ch",
}
SCENARIO_PREFIX = "Test_FaultKnown_"


def scenario_titles():
    """Maps each scenario test's method suffix to its scenario name. The method
    name is the name in CamelCase, so fault_known.go is the only list."""
    names = re.findall(r'Name: "([^"]+)"', KNOWN_FAILURES.read_text())
    return {"".join(w[0].upper() + w[1:] for w in n.split()): n for n in names}


def violations(text):
    """Harness violation messages in a failure, in the order the harness reported them."""
    found = []
    for line in text.splitlines():
        m = VIOLATION.search(line.strip())
        if m and "VIOLATION " not in line:
            found.append(m.group(1))
    return list(dict.fromkeys(found))


def signature(message):
    """A violation message without the case-specific parts, for grouping."""
    props, _, rest = message.partition("] ")
    # Progress detail: "domains=map[...]" in older runs, catalog column names now.
    s = re.sub(r"\(?(domains=map\[|sync_batch_id=\d|table \S+: synced_batch_id=).*", "", rest)
    s = re.sub(r"\{[^{}]*\}", "{…}", s)
    s = re.sub(r"(\w+\.)?(fault_\w+?_dst|do_not_remove\S+)", "<table>", s)
    s = re.sub(r"(?<![\w.])\d+(?![\w.])", "N", s)
    return props + "] " + s.strip(" :;(")


def is_case(name):
    """Leaf test of a scenario test or a generated case, not a parent test."""
    return name.startswith("Test") and name.count("/") >= (1 if SCENARIO_PREFIX in name else 2)


def workload(package, name):
    leaf = name.split("/")[-1]
    return leaf.split("__")[0] if "__" in leaf else PACKAGE_WORKLOAD.get(package, package)


def failing_cases(root):
    runs, _ = collect(root)
    return sorted(
        f"{package} {name}"
        for (package, name), attempts in runs.items()
        if is_case(name) and attempts[-1][0] == "fail"
    )


def comment(root):
    runs, found = collect(root)
    missing = sorted(set(os.environ.get("EXPECTED_RESULTS", "").split()) - found)
    run_url = os.environ.get("RUN_URL", "")
    titles = scenario_titles()
    order = {n: i for i, n in enumerate(titles.values())}

    counts = defaultdict(lambda: defaultdict(int))
    scenarios, groups, flaky, quota = [], defaultdict(list), [], []
    skipped_by_site = defaultdict(int)
    for (package, name), attempts in sorted(runs.items()):
        if not is_case(name):
            continue
        outcomes = [o for o, _ in attempts]
        failure = next((t for o, t in attempts if o == "fail"), "")
        w = workload(package, name)
        leaf = name.split("/")[-1]
        if SCENARIO_PREFIX in name:
            counts[w]["scenarios"] += 1
            first = (violations(failure) or ["see logs"])[0].replace("|", "\\|")
            if outcomes[-1] == "fail":
                counts[w]["scenarios_failing"] += 1
                result = f"❌ Failed: `{first}`"
            elif "fail" in outcomes:
                result = f"⚠️ Failed, then passed: `{first}`"
            elif outcomes[-1] == "skip":
                result = "⏭️ Not reached"
            else:
                result = "✅ Passed"
            suffix = leaf.removeprefix(SCENARIO_PREFIX)
            scenarios.append((titles.get(suffix, suffix), w, result))
            continue
        if "fail" not in outcomes:
            counts[w][outcomes[-1]] += 1
            if outcomes[-1] == "skip":
                m = NOT_REACHED_SITES.search(attempts[-1][1] or "")
                # A second site armed to fire after the first is never reached
                # when the first is not, so the first listed site is the cause.
                skipped_by_site[m.group(1).split(", ")[0] if m else "(no reason given)"] += 1
            continue
        found_violations = violations(failure)
        if QUOTA.search(failure):
            counts[w]["quota"] += 1
            quota.append(leaf)
        elif outcomes[-1] == "fail":
            counts[w]["fail"] += 1
            key = tuple(sorted({signature(v) for v in found_violations})) or ("(no harness violation; see logs)",)
            groups[key].append(leaf)
        else:
            counts[w]["flaky"] += 1
            flaky.append((leaf, " → ".join(outcomes), (found_violations or ["see logs"])[0]))

    out = [MARKER, "## Fault-injection results", ""]
    if missing:
        out += [f"**No results from:** {', '.join(missing)} (the job failed before uploading; see the run).", ""]
    out += [
        "| Workload | Scenario tests failing | Generated: failed every time | Failed, then passed | Passed | Skipped (not reached) |",
        "|---|---|---|---|---|---|",
    ]
    total = defaultdict(int)
    for w in sorted(counts):
        c = counts[w]
        for k, v in c.items():
            total[k] += v
        out.append(f"| {w} | {c['scenarios_failing']}/{c['scenarios']} | {c['fail']} | {c['flaky']} | {c['pass']} | {c['skip']} |")
    out.append(
        f"| **total** | **{total['scenarios_failing']}/{total['scenarios']}** | **{total['fail']}** "
        f"| **{total['flaky']}** | **{total['pass']}** | **{total['skip']}** |"
    )
    out.append("")

    out += ["### Scenario tests", "", "| Scenario | Workload | Result |", "|---|---|---|"]
    for title, w, result in sorted(scenarios, key=lambda r: (order.get(r[0], len(order)), r[1])):
        out.append(f"| {title} | {w} | {result} |")
    out.append("")

    def group_sections(with_names):
        section = ["### Generated cases that failed every time", ""]
        for key, names in sorted(groups.items(), key=lambda kv: (-len(kv[1]), kv[0])):
            sites = defaultdict(int)
            for n in names:
                for site in n.split("__")[-1].split("+"):
                    sites[site] += 1
            top = ", ".join(f"`{site}` ({k})" for site, k in sorted(sites.items(), key=lambda kv: (-kv[1], kv[0]))[:4])
            section += [f"<details><summary><b>{len(names)} cases</b>: <code>{html.escape(' + '.join(key))}</code></summary>", ""]
            section += [f"Sites in most of these cases: {top}", ""]
            if with_names:
                section += ["```", *sorted(names), "```"]
            else:
                section += ["Case names are in `failing-cases.txt` in the `fault-summary` artifact."]
            section += ["</details>", ""]
        return section

    tail = []
    if flaky:
        tail += ["### Failed, then passed on rerun", ""]
        tail += [f"- `{leaf}` ({seq}): `{first}`" for leaf, seq, first in sorted(flaky)]
        tail.append("")
    if quota:
        tail += ["### BigQuery quota errors", ""] + [f"- `{leaf}`" for leaf in sorted(quota)] + [""]
    if skipped_by_site:
        tail += ["### Skipped (not reached)", ""]
        if any(site.startswith("cdc.schema_only.") for site in skipped_by_site):
            tail += [
                "The `cdc.schema_only.*` hooks fire only for a batch that has a schema change and no rows. "
                "No PeerDB CDC connector produces such a batch, so cases with these hooks are expected to skip.",
                "",
            ]
        tail += ["| Site that never fired | Cases skipped |", "|---|---|"]
        tail += [f"| `{site}` | {n} |" for site, n in sorted(skipped_by_site.items(), key=lambda kv: (-kv[1], kv[0]))]
        tail.append("")
    tail.append(
        f"Full JUnit files and logs: [workflow run artifacts]({run_url}). "
        "Names of the cases that failed every time: `failing-cases.txt` in the `fault-summary` artifact."
    )

    body = "\n".join(out + group_sections(True) + tail) + "\n"
    if len(body) > GITHUB_COMMENT_LIMIT:
        # Only when nearly every case fails: keep every group, drop the name lists.
        body = "\n".join(out + group_sections(False) + tail) + "\n"
    return body


def main():
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    command, arg = sys.argv[1], sys.argv[2]
    if command == "failed":
        print("\n".join(failed(arg)))
    elif command == "rerun-pattern":
        print(rerun_pattern(arg))
    elif command == "comment":
        sys.stdout.write(comment(arg))
    elif command == "failing-cases":
        print("\n".join(failing_cases(arg)))
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
