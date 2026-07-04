package compare

import (
	"errors"
	"testing"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/digest"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/run"
)

func BenchmarkBehaviorKeyAccept(b *testing.B) {
	c := run.Case{SQL: []byte("ALTER TABLE db1.t1 ADD COLUMN c1 INT NOT NULL, DROP COLUMN c2"), Engine: run.EngineMySQL, SQLMode: SQLModeANSIQuotes}
	d := acceptAlter(
		digest.Spec{Op: "add", Cols: []digest.Col{{Name: "c1", TypeStr: "int"}}},
		digest.Spec{Op: "drop", Cols: []digest.Col{{Name: "c2", TypeStr: "int"}}},
	)
	sig := "alter db1.t1{col c1=int32 notnull;drop c2}"
	b.ReportAllocs()
	for b.Loop() {
		BehaviorKey(c, sig, nil, nil, d)
	}
}

func BenchmarkBehaviorKeyReject(b *testing.B) {
	c := run.Case{SQL: []byte("ALTER TABLE t1 ADD COLUMN"), Engine: run.EngineMySQL}
	d := &digest.Digest{Verdict: "reject", Error: `You have an error in your SQL syntax near 'ADD COLUMN' at line 1`}
	err := errors.New(`parse error at byte 21: unexpected end of input`)
	b.ReportAllocs()
	for b.Loop() {
		BehaviorKey(c, "", err, nil, d)
	}
}
