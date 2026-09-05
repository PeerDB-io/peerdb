#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
flow_dir="${script_dir}/flow"
bench_count="${BENCH_COUNT:-5}"
bench_time="${BENCH_TIME:-500ms}"

if [[ -n "${RESULTS_FILE:-}" ]]; then
	results_file="${RESULTS_FILE}"
	: >"${results_file}"
else
	results_file="$(mktemp -t peerdb-postgres-json-bench.XXXXXX)"
	trap 'rm -f "${results_file}"' EXIT
fi

failures=0

run_benchmark() {
	if ! (cd "${flow_dir}" && go test ./connectors/postgres -run '^$' "$@") \
		| tee -a "${results_file}" >&2; then
		failures=1
	fi
}

echo "Running regular inputs: count=${bench_count}, benchtime=${bench_time}" >&2
run_benchmark \
	-bench '^(BenchmarkPostgresJSONPreparation|BenchmarkJsonStringHandling)$' \
	-benchmem \
	-count "${bench_count}" \
	-benchtime "${bench_time}"

generated_benchmarks='BenchmarkRelaxedNumberExtension|BenchmarkConvertRelaxedNumberNormalizeStrings|BenchmarkConvertRelaxedNumberRawCopyPreserveInvalidUnicode|BenchmarkConvertRelaxedNumberRawCopyWithRepair'

for shape in '4 32' '8 4' '8 8' '64 2'; do
	read -r num_fields max_depth <<<"${shape}"
	printf -v pattern '^(%s)$/^numFields=%s$/^maxDepth=%s$' \
		"${generated_benchmarks}" "${num_fields}" "${max_depth}"
	echo "Running generated ${num_fields}/${max_depth}: count=${bench_count}, benchtime=${bench_time}" >&2
	run_benchmark \
		-bench "${pattern}" \
		-benchmem \
		-count "${bench_count}" \
		-benchtime "${bench_time}"
done

# These inputs are approximately 3.36 GB and 930 MB. Run every implementation
# in a fresh process and force exactly one measured iteration to avoid benchmark
# calibration multiplying their memory use.

if [[ "${SKIP_GIANT:-0}" != 1 ]]; then
	for shape in '8 16' '64 4'; do
		read -r num_fields max_depth <<<"${shape}"
		for benchmark in \
			BenchmarkRelaxedNumberExtension \
			BenchmarkConvertRelaxedNumberNormalizeStrings \
			BenchmarkConvertRelaxedNumberRawCopyPreserveInvalidUnicode \
			BenchmarkConvertRelaxedNumberRawCopyWithRepair; do
			printf -v pattern '^%s$/^numFields=%s$/^maxDepth=%s$' \
				"${benchmark}" "${num_fields}" "${max_depth}"
			echo "Running generated ${num_fields}/${max_depth}: ${benchmark}, one iteration" >&2
			run_benchmark \
				-bench "${pattern}" \
				-benchmem \
				-count 1 \
				-benchtime 1x
		done
	done
fi

awk '
function modeFor(top) {
	if (top == "BenchmarkRelaxedNumberExtension")
		return "legacy-object-decode-encode"
	if (top == "BenchmarkConvertRelaxedNumberNormalizeStrings")
		return "json-token-decode-encode"
	if (top == "BenchmarkConvertRelaxedNumberRawCopyPreserveInvalidUnicode")
		return "raw-copy-preserve-invalid-unicode"
	if (top == "BenchmarkConvertRelaxedNumberRawCopyWithRepair")
		return "raw-copy-with-unicode-repair"
	return ""
}

function median(key, count, i, j, tmp) {
	count = samples[key]
	if (count == 0)
		return 0
	for (i = 1; i <= count; i++) {
		for (j = i + 1; j <= count; j++) {
			if (values[key, j] < values[key, i]) {
				tmp = values[key, i]
				values[key, i] = values[key, j]
				values[key, j] = tmp
			}
		}
	}
	if (count % 2 == 1)
		return values[key, (count + 1) / 2]
	return (values[key, count / 2] + values[key, count / 2 + 1]) / 2
}

function formatTime(ns) {
	if (ns < 1000)
		return sprintf("%.1f ns", ns)
	if (ns < 1000000)
		return sprintf(ns < 10000 ? "%.2f µs" : "%.1f µs", ns / 1000)
	if (ns < 1000000000)
		return sprintf("%.2f ms", ns / 1000000)
	return sprintf("%.2f s", ns / 1000000000)
}

function formatSize(bytes) {
	if (bytes < 1000)
		return sprintf("%d B", bytes)
	if (bytes < 1000000)
		return sprintf("%.1f KB", bytes / 1000)
	if (bytes < 1000000000)
		return sprintf("%.2f MB", bytes / 1000000)
	return sprintf("%.2f GB", bytes / 1000000000)
}

function result(payload, mode, legacy, value, key) {
	key = payload SUBSEP mode
	value = median(key)
	if (value == 0)
		return "missing"
	if (mode == "legacy-object-decode-encode")
		return formatTime(value)
	return sprintf("%s · **%.2f×**", formatTime(value), legacy / value)
}

function addRow(payload, label) {
	rowCount++
	rowPayload[rowCount] = payload
	rowLabel[rowCount] = label
}

$1 ~ /^Benchmark/ && $4 == "ns\/op" {
	name = $1
	sub(/-[0-9]+$/, "", name)
	partsCount = split(name, parts, "/")
	top = parts[1]

	if (top == "BenchmarkPostgresJSONPreparation" || top == "BenchmarkJsonStringHandling") {
		payload = parts[2]
		mode = parts[3]
	} else {
		mode = modeFor(top)
		if (mode == "" || partsCount < 3)
			next
		sub(/^numFields=/, "", parts[2])
		sub(/^maxDepth=/, "", parts[3])
		payload = "generated-" parts[2] "-" parts[3]
	}

	key = payload SUBSEP mode
	samples[key]++
	values[key, samples[key]] = $3 + 0
	for (i = 5; i <= NF; i++) {
		if ($i == "B/input") {
			sizes[payload] = $(i - 1) + 0
			break
		}
	}
}

END {
	addRow("small-no-substitution", "Small")
	addRow("small-one-substitution", "Small + long number")
	addRow("1MiB-no-substitution", "1 MiB")
	addRow("1MiB-one-substitution", "1 MiB + long number")
	addRow("issues-no-substitution", "Issues")
	addRow("issues-middle-long-number", "Issues + long number")
	addRow("issues-invalid-unicode", "Issues + invalid Unicode")
	addRow("commits-no-substitution", "Commits")
	addRow("commits-middle-long-number", "Commits + long number")
	addRow("commits-invalid-unicode", "Commits + invalid Unicode")
	addRow("events-no-substitution", "Events")
	addRow("events-middle-long-number", "Events + long number")
	addRow("events-invalid-unicode", "Events + invalid Unicode")
	addRow("escaped-unicode", "Escaped Unicode")
	addRow("invalid-utf8", "Invalid UTF-8")
	addRow("lone-surrogate", "Lone surrogate")
	addRow("generated-4-32", "Generated 4/32")
	addRow("generated-8-4", "Generated 8/4")
	addRow("generated-8-8", "Generated 8/8")
	addRow("generated-8-16", "Generated 8/16")
	addRow("generated-64-2", "Generated 64/2")
	addRow("generated-64-4", "Generated 64/4")

	print "| Payload | Size | Legacy | Committed | Raw patch | Raw + repair |"
	print "|---|---:|---:|---:|---:|---:|"
	for (i = 1; i <= rowCount; i++) {
		payload = rowPayload[i]
		legacy = median(payload SUBSEP "legacy-object-decode-encode")
		printf "| %s | %s | %s | %s | %s | %s |\n", \
			rowLabel[i], formatSize(sizes[payload]), \
			result(payload, "legacy-object-decode-encode", legacy), \
			result(payload, "json-token-decode-encode", legacy), \
			result(payload, "raw-copy-preserve-invalid-unicode", legacy), \
			result(payload, "raw-copy-with-unicode-repair", legacy)
	}
}
' "${results_file}"

if ((failures != 0)); then
	echo "One or more benchmark processes failed; missing cells are marked in the table." >&2
	exit 1
fi
