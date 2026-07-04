package mutate

import (
	"bytes"
	"math/rand/v2"
	"testing"
	"unicode/utf8"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/run"
)

func TestMutationByteSafety(t *testing.T) {
	base := run.Case{SQL: []byte("ALTER TABLE t ADD c INT"), Engine: run.EngineMySQL}
	mate := run.Case{SQL: []byte("ALTER TABLE u DROP COLUMN d"), Engine: run.EngineMySQL}
	for i := range 2000 {
		got := MutateWithSplice(rand.New(rand.NewPCG(uint64(i), uint64(i+1))), base, &mate)
		if !utf8.Valid(got.SQL) {
			t.Fatalf("invalid utf8 at %d: %q", i, got.SQL)
		}
		if bytes.Contains(got.SQL, []byte(sentinelDB)) {
			t.Fatalf("sentinel emitted at %d: %q", i, got.SQL)
		}
	}
}

func TestMutationOpsDropUnsafeBytes(t *testing.T) {
	prev := []byte("ALTER TABLE t ADD c INT")
	if got := validOrPrevious(prev, []byte{0xff}); string(got) != string(prev) {
		t.Fatalf("invalid utf8 kept: %q", got)
	}
	if got := validOrPrevious(prev, []byte("ALTER TABLE peerdb_ddlfuzz_nodb.t ADD c INT")); string(got) != string(prev) {
		t.Fatalf("sentinel kept: %q", got)
	}
}

func TestDictionaryInsertAndCrossover(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	sql := []byte("ALTER TABLE t ADD c INT")
	if got := dictionaryInsert(r, sql, "mysql"); !utf8.Valid(got) || len(got) <= len(sql) {
		t.Fatalf("dictionaryInsert got %q", got)
	}
	cross := Crossover(rand.New(rand.NewPCG(3, 4)), []byte("ALTER TABLE t ADD c INT"), []byte("RENAME TABLE a TO b"))
	if !utf8.Valid(cross) || len(cross) == 0 {
		t.Fatalf("bad crossover: %q", cross)
	}
}
