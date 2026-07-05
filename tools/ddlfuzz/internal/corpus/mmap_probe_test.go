package corpus

import (
	"path/filepath"
	"testing"
)

func TestMmapPragmaEffective(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "probe.db"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var got int64
	if err := s.db.QueryRow(`PRAGMA mmap_size`).Scan(&got); err != nil {
		t.Fatal(err)
	}
	t.Logf("effective mmap_size = %d", got)
	if got <= 0 {
		t.Fatalf("mmap_size pragma had no effect: %d", got)
	}
}
