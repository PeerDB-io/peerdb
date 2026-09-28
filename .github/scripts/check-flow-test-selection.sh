#!/usr/bin/env bash
# The unit job skips tilt-tagged test files. Make sure every package containing
# those tests belongs to exactly one source job in flow.yml. Also make sure
# every matrix target has a source job entry; without one, it could run no
# tests and still pass.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.."
repo=$PWD
workflow="$repo/.github/workflows/flow.yml"
table='.jobs.flow_test.strategy.matrix.jobs[0]'
jobs=$(yq -r "$table | keys | .[]" "$workflow" | sort)
targets=$(yq -r '.jobs.flow_test.strategy.matrix.target[].job' "$workflow" | sort -u)
if ! diff <(printf '%s\n' "$jobs") <(printf '%s\n' "$targets") >&2; then
  echo 'Flow test targets and source jobs differ (see diff above)' >&2
  exit 1
fi

# Emit package<TAB>test file pairs: tagged tests can join packages with unit tests.
test_files() {
  # shellcheck disable=SC2016 # Go template variables must remain literal.
  go list -f '{{range .TestGoFiles}}{{printf "%s\t%s\n" $.ImportPath .}}{{end}}{{range .XTestGoFiles}}{{printf "%s\t%s\n" $.ImportPath .}}{{end}}' "$@" ./... | sed '/^$/d' | sort -u
}
job_patterns() {
  yq -r "$table | to_entries[] | .key + \" \" + ($1)" "$workflow"
}

# Check one module against its source-job package lists. The second argument
# selects the flow or flow/pkg package list.
check_module() (
  local dir=$1 pattern_columns=$2 base tagged must jobs_input owners='' job patterns listed missing duplicates
  local -a args
  cd "$repo/$dir"
  base=$(test_files)
  tagged=$(test_files -tags tilt)
  must=$(comm -13 <(printf '%s\n' "$base") <(printf '%s\n' "$tagged") | cut -f1 | sort -u)

  jobs_input=$(job_patterns "$pattern_columns")
  while read -r job patterns; do
    if [[ ! "$patterns" =~ [^[:space:]] ]]; then
      continue
    fi
    read -r -a args <<< "$patterns"
    if ! listed=$(go list -tags tilt "${args[@]}"); then
      echo "job $job: invalid $dir package pattern" >&2
      exit 1
    fi
    if [[ -z "$listed" ]]; then
      echo "job $job: $dir pattern matched no packages" >&2
      exit 1
    fi
    if [[ -n "$owners" ]]; then
      owners+=$'\n'
    fi
    owners+="$listed"
  done <<< "$jobs_input"

  missing=$(comm -23 <(printf '%s\n' "$must") <(printf '%s\n' "$owners" | sort -u))
  duplicates=$(printf '%s\n' "$owners" | sort | uniq -d)
  if [[ -n "$missing" || -n "$duplicates" ]]; then
    if [[ -n "$missing" ]]; then
      printf '%s\n' "$missing" | sed 's/^/unowned: /' >&2
    fi
    if [[ -n "$duplicates" ]]; then
      printf '%s\n' "$duplicates" | sed 's/^/owned twice: /' >&2
    fi
    exit 1
  fi
  echo "$dir: all test packages are owned once"
)

check_module flow '.value.packages // ""'
check_module flow/pkg '.value.pkg // ""'
