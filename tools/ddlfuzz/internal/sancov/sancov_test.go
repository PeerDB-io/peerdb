package sancov

import "testing"

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
