package compare

import (
	"errors"
	"testing"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/digest"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/run"
)

func BenchmarkBehaviorFeatureAccept(b *testing.B) {
	c := run.Case{SQL: []byte("ALTER TABLE db1.t1 ADD COLUMN c1 INT NOT NULL, DROP COLUMN c2"), Engine: run.EngineMySQL, SQLMode: SQLModeANSIQuotes}
	d := acceptAlter(
		digest.Spec{Op: "add", Cols: []digest.Col{{Name: "c1", TypeStr: "int"}}},
		digest.Spec{Op: "drop", Cols: []digest.Col{{Name: "c2", TypeStr: "int"}}},
	)
	sig := "alter db1.t1{col c1=int32 nn; drop c2}"
	var buf []uint64
	b.ReportAllocs()
	for b.Loop() {
		buf = BehaviorFeatures(c, sig, nil, nil, d, buf[:0])
	}
}

func BenchmarkBehaviorFeatureReject(b *testing.B) {
	c := run.Case{SQL: []byte("ALTER TABLE t1 ADD COLUMN"), Engine: run.EngineMySQL}
	d := &digest.Digest{Verdict: "reject", Error: `You have an error in your SQL syntax near 'ADD COLUMN' at line 1`}
	err := errors.New(`parse error at byte 21: unexpected end of input`)
	var buf []uint64
	b.ReportAllocs()
	for b.Loop() {
		buf = BehaviorFeatures(c, "", err, nil, d, buf[:0])
	}
}
