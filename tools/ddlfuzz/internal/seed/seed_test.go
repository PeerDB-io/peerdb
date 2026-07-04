package seed

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestExtractSeeds(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)
	flowDir := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", "..", "flow", "connectors", "mysql"))
	seeds, err := Extract(flowDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(seeds) < 600 {
		t.Fatalf("got %d seeds, want >=600", len(seeds))
	}
	want := map[string]struct{}{
		`ALTER TABLE "db"."t" ADD COLUMN "c" INT NOT NULL|mysql|4`:      {},
		`ALTER TABLE [db].[t] ADD COLUMN [c] INT NOT NULL|mariadb|1024`: {},
		`ALTER TABLE t MODIFY a NUMBER(10,2)|mariadb|512`:               {},
	}
	for _, s := range seeds {
		delete(want, s.SQL+"|"+s.Engine+"|"+itoa(s.SQLMode))
	}
	if len(want) != 0 {
		t.Fatalf("missing mode seeds: %#v", want)
	}
}

func TestLoadDirIncludesFixSQLSeeds(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "abc123.sql"), []byte("ALTER TABLE t ADD c INT"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "abc123.meta.json"), []byte(`{"engine":"mariadb","sql_mode":512,"expect_sig":"sig"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	seeds, err := LoadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(seeds) != 1 {
		t.Fatalf("got %d seeds, want 1", len(seeds))
	}
	got := seeds[0]
	if got.SQL != "ALTER TABLE t ADD c INT" || got.Engine != "mariadb" || got.SQLMode != 512 || got.ExpectSig != "sig" || got.Source != "abc123.sql" {
		t.Fatalf("bad seed: %+v", got)
	}
}

func itoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}
