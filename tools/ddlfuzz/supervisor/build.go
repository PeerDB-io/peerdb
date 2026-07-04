package main

// ddlfuzzCoverBuildArgs returns the `go` arguments for building the ddlfuzz
// binary with parser coverage enabled. -covermode=atomic is required by
// runtime/coverage.WriteCounters (the fuzzer's parser-coverage feedback poll),
// and cmd/ddlfuzz — a thin main shim — must be in -coverpkg because the
// compiler only injects the coverage init hook into an instrumented main
// package; without it WriteCounters always fails. Keep build/build.sh in sync.
func ddlfuzzCoverBuildArgs(outBin string) []string {
	return []string{
		"build",
		"-cover",
		"-covermode=atomic",
		"-coverpkg=github.com/PeerDB-io/peerdb/flow/connectors/mysql,github.com/PeerDB-io/peerdb/tools/ddlfuzz/cmd/ddlfuzz",
		"-o", outBin,
		"./cmd/ddlfuzz",
	}
}
