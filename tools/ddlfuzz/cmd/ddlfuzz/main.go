// Package main is a thin shim over internal/fuzzcmd. It must stay minimal:
// runtime/coverage.WriteCounters (the parser-coverage feedback signal) only
// works when the main package itself is instrumented — the compiler injects
// the coverage init hook into main's init — so this package is listed in
// -coverpkg alongside the parser. Keeping it a one-liner means the fuzzer
// harness itself contributes no meaningful coverage counters.
package main

import "github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/fuzzcmd"

func main() {
	fuzzcmd.Main()
}
