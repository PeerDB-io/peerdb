package sancov

import (
	"math/rand/v2"
	"testing"
)

// TestAccumulatorMergeWords exercises the 8-byte word fast path (inputs larger
// than one word) and cross-checks it against a naive byte-wise reference.
func TestAccumulatorMergeWords(t *testing.T) {
	const n = 1000
	ref := make([]byte, n)
	refEdges := 0
	a := &Accumulator{Engine: "mysql"}
	rng := rand.New(rand.NewPCG(1, 2))
	for range 50 {
		snap := make([]byte, n)
		for i := range snap {
			if rng.IntN(20) == 0 {
				snap[i] = byte(1 + rng.IntN(255))
			}
		}
		wantGrew := false
		wantNew := 0
		for i, v := range snap {
			if v != 0 && ref[i] == 0 {
				wantGrew = true
				wantNew++
				refEdges++
			}
			ref[i] |= v
		}
		grew, got := a.Merge(snap)
		if grew != wantGrew || got != wantNew {
			t.Fatalf("merge grew=%v/%v new=%d/%d", grew, wantGrew, got, wantNew)
		}
		if a.EdgeCount() != refEdges {
			t.Fatalf("edges=%d want %d", a.EdgeCount(), refEdges)
		}
	}
}

func TestAccumulatorMerge(t *testing.T) {
	a := &Accumulator{Engine: "mysql"}
	grew, n := a.Merge([]byte{0, 1, 0, 2})
	if !grew || n != 2 || a.EdgeCount() != 2 {
		t.Fatalf("first merge grew=%v n=%d edges=%d", grew, n, a.EdgeCount())
	}
	grew, n = a.Merge([]byte{0, 3, 0, 4})
	if grew || n != 0 || a.EdgeCount() != 2 {
		t.Fatalf("second merge grew=%v n=%d edges=%d", grew, n, a.EdgeCount())
	}
	grew, n = a.Merge([]byte{1, 3, 0, 4, 5})
	if !grew || n != 2 || a.EdgeCount() != 4 {
		t.Fatalf("third merge grew=%v n=%d edges=%d", grew, n, a.EdgeCount())
	}
}
