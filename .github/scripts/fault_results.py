#!/usr/bin/env python3
"""Reads the JUnit files fault-run.sh writes.

  fault_results.py failed FILE           failed leaf tests in FILE, one per line
  fault_results.py rerun-pattern FILE    go test -run pattern rerunning them
  fault_results.py comment DIR           PR comment markdown for DIR, which holds
                                         one fault-results-<shard> directory per job

comment reads RUN_URL (link to the workflow run) and EXPECTED_RESULTS
(space-separated <shard> names that should have uploaded results) from the
environment.
"""

import os
import re
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path

MARKER = "<!-- fault-injection-results -->"
# GitHub rejects comments over 65,536 characters; leave room for the footer.
COMMENT_BUDGET = 60_000
ERROR_LINES = 5
ERROR_LINE_CHARS = 300
ATTEMPT_FILE = re.compile(r"attempt(\d+)-(.+)\.xml$")
QUOTA = re.compile(r"quota|rateLimitExceeded", re.IGNORECASE)
# Lines worth showing first: harness violations, testify failures, panics.
INTERESTING = re.compile(r"VIOLATION |Error Trace:|Error:|panic:|NOT REACHED|setup failure")
TEST_EVENT = re.compile(r"^(=== (RUN|PAUSE|CONT|NAME)|--- (FAIL|PASS|SKIP))")


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


def excerpt(text):
    lines = [line.strip() for line in text.splitlines() if line.strip() and not TEST_EVENT.match(line.strip())]
    picked = []
    for line in lines:
        if INTERESTING.search(line) and line not in picked:
            picked.append(line)
    if not picked:
        picked = lines[-ERROR_LINES:]
    return [line[:ERROR_LINE_CHARS] for line in picked[:ERROR_LINES]]


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


def comment(root):
    runs, found = collect(root)
    expected = os.environ.get("EXPECTED_RESULTS", "").split()
    missing = sorted(set(expected) - found)
    run_url = os.environ.get("RUN_URL", "")

    groups = {"always": defaultdict(list), "flaky": defaultdict(list), "quota": defaultdict(list)}
    counts = defaultdict(int)
    for (package, name), attempts in sorted(runs.items()):
        if not name.startswith("Test") and not name.startswith("<"):
            continue
        outcomes = [o for o, _ in attempts]
        if "fail" not in outcomes:
            counts[outcomes[-1]] += 1
            continue
        text = next(t for o, t in attempts if o == "fail")
        if QUOTA.search(text):
            kind = "quota"
        elif outcomes[-1] == "fail":
            kind = "always"
        else:
            kind = "flaky"
        counts[kind] += 1
        groups[kind][package].append((name, " → ".join(outcomes), excerpt(text)))

    out = [
        MARKER,
        "## Fault-injection results",
        "",
        f"{sum(counts.values())} tests: {counts['pass']} passed, {counts['skip']} skipped (mostly NOT REACHED), "
        f"**{counts['always']} failed every time**, {counts['flaky']} failed then passed on rerun, "
        f"{counts['quota']} hit BigQuery quota errors.",
    ]
    if missing:
        out += ["", f"**No results from:** {', '.join(missing)} (job failed before uploading; see the run)."]
    footer = ["", f"Full JUnit files and logs: [workflow run artifacts]({run_url})."]

    titles = {
        "always": "Failed every time",
        "flaky": "Failed, then passed on rerun",
        "quota": "BigQuery quota errors",
    }
    size = sum(len(line) + 1 for line in out + footer)
    omitted = 0
    for kind in ("always", "flaky", "quota"):
        if not groups[kind]:
            continue
        section = ["", f"### {titles[kind]}"]
        for package in sorted(groups[kind]):
            section += ["", f"#### {package}"]
            for name, sequence, lines in groups[kind][package]:
                entry = ["", f"<details><summary><code>{name}</code> ({sequence})</summary>", "", "```"]
                entry += [line.replace("```", "'''") for line in lines]
                entry += ["```", "</details>"]
                entry_size = sum(len(line) + 1 for line in entry)
                if size + entry_size + sum(len(line) + 1 for line in section) > COMMENT_BUDGET:
                    omitted += 1
                    continue
                section += entry
        size += sum(len(line) + 1 for line in section)
        out += section
    if omitted:
        out += ["", f"**{omitted} more failures omitted to fit the comment size limit.**"]
    return "\n".join(out + footer) + "\n"


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
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
