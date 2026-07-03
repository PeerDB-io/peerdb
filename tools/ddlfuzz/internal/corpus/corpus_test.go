package corpus

import (
	"bytes"
	"math/rand/v2"
	"path/filepath"
	"testing"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/run"
)

func mkCase(n int, size int) run.Case {
	sql := bytes.Repeat([]byte{'a'}, size)
	sql[0] = byte(n)
	return run.Case{SQL: sql, SQLMode: uint64(n)}
}

func TestBudgetRefusesWhenFull(t *testing.T) {
	path := filepath.Join(t.TempDir(), "corpus.db")
	s, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if ok, err := s.Add(mkCase(1, 10)); err != nil || !ok {
		t.Fatalf("initial add: ok=%v err=%v", ok, err)
	}
	s.Budget = s.Bytes()
	if ok, _ := s.Add(mkCase(2, 10)); ok {
		t.Fatal("add over budget succeeded")
	}
	if s.SkippedFull() != 1 {
		t.Fatalf("SkippedFull = %d, want 1", s.SkippedFull())
	}
	// duplicates are still deduped, not counted as budget refusals
	if ok, _ := s.Add(mkCase(1, 10)); ok {
		t.Fatal("duplicate add succeeded")
	}
	if s.SkippedFull() != 1 {
		t.Fatalf("SkippedFull after dup = %d, want 1", s.SkippedFull())
	}
}

func TestReopenKeepsEntriesAndBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "corpus.db")
	s, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := s.Add(mkCase(1, 5000)); err != nil || !ok {
		t.Fatalf("add: ok=%v err=%v", ok, err)
	}
	if got := s.Bytes(); got <= 0 {
		t.Fatalf("Bytes = %d, want > 0", got)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if got := s2.Count("mysql"); got != 1 {
		t.Fatalf("reopened Count(mysql) = %d, want 1", got)
	}
	if got := s2.Bytes(); got <= 0 {
		t.Fatalf("reopened Bytes = %d, want > 0", got)
	}
}

func TestRandomSQL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "corpus.db")
	s, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := range 5 {
		if ok, err := s.Add(mkCase(i, 10)); err != nil || !ok {
			t.Fatalf("add %d: ok=%v err=%v", i, ok, err)
		}
	}
	got, ok, err := s.RandomSQL("mysql", rand.New(rand.NewPCG(1, 2)))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || len(got) != 10 {
		t.Fatalf("RandomSQL ok=%v len=%d, want ok len 10", ok, len(got))
	}
	if _, ok, err := s.RandomSQL("mariadb", rand.New(rand.NewPCG(1, 2))); err != nil || ok {
		t.Fatalf("RandomSQL missing engine ok=%v err=%v, want false nil", ok, err)
	}
}
