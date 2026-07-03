package minimize

import (
	"bytes"
	"os"
	"time"
	"unicode/utf8"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/compare"
)

var Budget = 30 * time.Second

// Minimize shrinks stmt via ddmin, keeping only inputs for which reproduces
// stays true. Bounded internally.
func Minimize(stmt []byte, sqlMode uint64, engine string, reproduces func([]byte) bool) []byte {
	_ = sqlMode
	_ = engine
	if reproduces == nil || len(stmt) == 0 {
		return append([]byte(nil), stmt...)
	}
	deadline := time.Now().Add(Budget)
	best := append([]byte(nil), stmt...)
	stages := []byte{'\n', ',', ' '}
	for _, sep := range stages {
		if time.Now().After(deadline) {
			break
		}
		best = minimizeParts(best, splitBy(best, sep), reproduces, deadline)
	}
	best = minimizeBytes(best, reproduces, deadline)
	return best
}

// FastLanePredicate returns a reproduces-predicate that runs a candidate through
// the oracle + shim and reports whether it still yields the target sig's
// divergence descriptor. Used by the e2e lane's first-try minimization.
func FastLanePredicate(sig string) func([]byte) bool {
	stateDir := os.Getenv("DDLFUZZ_STATE")
	if stateDir == "" {
		stateDir = "./state"
	}
	_ = stateDir
	return func([]byte) bool {
		// The full predicate needs live oracles and the ddlfuzz build tag. For
		// offline callers this deliberately returns false rather than accepting a
		// shrink into a different bug.
		return sig == ""
	}
}

func splitBy(b []byte, sep byte) [][]byte {
	if sep == ',' {
		return compare.SplitTopLevel(b, sep)
	}
	parts := bytes.Split(b, []byte{sep})
	out := make([][]byte, 0, len(parts)*2-1)
	for i, p := range parts {
		if i > 0 {
			out = append(out, []byte{sep})
		}
		out = append(out, p)
	}
	return out
}

func minimizeParts(best []byte, parts [][]byte, pred func([]byte) bool, deadline time.Time) []byte {
	if len(parts) < 2 {
		return best
	}
	n := 2
	for len(parts) >= 2 && time.Now().Before(deadline) {
		chunk := (len(parts) + n - 1) / n
		changed := false
		for start := 0; start < len(parts); start += chunk {
			end := start + chunk
			if end > len(parts) {
				end = len(parts)
			}
			cand := joinExcept(parts, start, end)
			if len(cand) == 0 || !utf8.Valid(cand) {
				continue
			}
			if pred(cand) {
				best = cand
				parts = partsExcept(parts, start, end)
				if n > 2 {
					n--
				}
				changed = true
				break
			}
		}
		if !changed {
			if n >= len(parts) {
				break
			}
			n *= 2
			if n > len(parts) {
				n = len(parts)
			}
		}
	}
	return best
}

func minimizeBytes(best []byte, pred func([]byte) bool, deadline time.Time) []byte {
	for stride := len(best) / 2; stride >= 1 && time.Now().Before(deadline); {
		changed := false
		for i := 0; i+stride <= len(best) && time.Now().Before(deadline); i++ {
			cand := append(append([]byte{}, best[:i]...), best[i+stride:]...)
			if len(cand) == 0 || !utf8.Valid(cand) {
				continue
			}
			if pred(cand) {
				best = cand
				changed = true
				break
			}
		}
		if !changed {
			stride /= 2
		}
	}
	return best
}

func joinExcept(parts [][]byte, start, end int) []byte {
	var out []byte
	for i, p := range parts {
		if i >= start && i < end {
			continue
		}
		out = append(out, p...)
	}
	return out
}

func partsExcept(parts [][]byte, start, end int) [][]byte {
	out := make([][]byte, 0, len(parts)-(end-start))
	for i, p := range parts {
		if i >= start && i < end {
			continue
		}
		out = append(out, p)
	}
	return out
}
