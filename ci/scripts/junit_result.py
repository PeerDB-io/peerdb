#!/usr/bin/env python3
"""JUnit XML -> praktika Result translator (for `go test` / gotestsum output).

praktika ships gtest and pytest translators (Result.from_gtest_run /
from_pytest_run) but no JUnit one. Our Go suites emit JUnit XML via
`gotestsum --junitfile`. This builds a praktika Result whose sub-results are the
individual test cases, so the report page renders a per-test table (failures
first) instead of a single pass/fail synthesized from the job exit code.

Reusable across every JUnit-producing job (unit tests, the Tilt e2e suites).

JUnit shape consumed (gotestsum's output):

    <testsuites>
      <testsuite name="pkg/path">
        <testcase classname="pkg/path" name="TestXxx" time="12.3">
          <failure message="...">...body...</failure>   # or <error>/<skipped>
        </testcase>
      </testsuite>
    </testsuites>
"""
import glob
import xml.etree.ElementTree as ET

from praktika.result import Result

# JUnit child tag -> praktika status. A <testcase> carries at most one of these;
# its absence means the case passed.
_TAG_STATUS = (
    ("failure", Result.Status.FAIL),
    ("error", Result.Status.ERROR),
    ("skipped", Result.Status.SKIPPED),
)


def _case_status(testcase):
    for tag, status in _TAG_STATUS:
        if testcase.find(tag) is not None:
            return status
    return Result.Status.OK


def _case_info(testcase):
    """Failure/error/skip message + body, for Result.info."""
    for tag, _ in _TAG_STATUS:
        el = testcase.find(tag)
        if el is not None:
            msg = (el.get("message") or "").strip()
            body = (el.text or "").strip()
            return "\n".join(p for p in (msg, body) if p)
    return ""


def _case_duration(testcase):
    try:
        return float(testcase.get("time") or 0)
    except ValueError:
        return None


def _parse_file(path):
    """Parse one JUnit file into a list of per-testcase Results."""
    results = []
    root = ET.parse(path).getroot()
    # root may be <testsuites> (wrapping many) or a lone <testsuite>; iter()
    # yields the element itself too, so both shapes are covered.
    for suite in root.iter("testsuite"):
        suite_name = suite.get("name", "")
        for tc in suite.findall("testcase"):
            # gotestsum puts the package in classname and the test in name;
            # join them for a readable, stable row label.
            prefix = tc.get("classname", "") or suite_name
            name = tc.get("name", "")
            full = " :: ".join(p for p in (prefix, name) if p) or name or "(unnamed)"
            results.append(
                Result(
                    name=full,
                    status=_case_status(tc),
                    duration=_case_duration(tc),
                    info=_case_info(tc),
                )
            )
    return results


def results_from_junit(report_glob):
    """Parse every JUnit file matching `report_glob` (recursive) into a flat list
    of per-testcase Results. Returns (results, matched_paths, error_str)."""
    paths = sorted(glob.glob(report_glob, recursive=True))
    if not paths:
        return [], [], f"no JUnit report matched [{report_glob}]"
    results = []
    for p in paths:
        try:
            results.extend(_parse_file(p))
        except Exception as e:  # malformed XML / truncated file
            return results, paths, f"failed to parse [{p}]: {e}"
    return results, paths, ""


def build_job_result(report_glob, run_exit_code, stopwatch, name="", extra_files=None):
    """Assemble a Result from JUnit reports, reconciled with the test process
    exit code. Returns a Result (the caller aggregates / completes the job).

    - No report / no cases (process killed before writing, or glob miss) -> ERROR.
    - Cases parsed -> status derived from the cases (any FAIL -> FAIL, etc.).
    - All cases green but the process exited non-zero (panic or teardown failure
      after the tests finished) -> force FAIL; a green suite must not mask it.
      (Mirrors Result.from_gtest_run's binary-failed reconciliation.)

    The matched JUnit files are always attached, so they are available as
    artifacts even if praktika can't yet render them natively.
    """
    results, paths, err = results_from_junit(report_glob)
    base_files = list(extra_files or [])
    if not results:
        return Result.create_from(
            name=name,
            status=Result.Status.ERROR,
            stopwatch=stopwatch,
            info=err or "JUnit report contained no test cases",
            files=base_files + paths,
        )
    result = Result.create_from(
        name=name,
        results=results,
        stopwatch=stopwatch,
        files=base_files + paths,
        info=err,  # non-fatal note if some (but not all) files failed to parse
    )
    if run_exit_code != 0 and result.is_ok():
        result.set_status(Result.Status.FAIL)
        result.set_info(
            f"test process exited with code [{run_exit_code}] despite all parsed cases passing"
        )
    return result
