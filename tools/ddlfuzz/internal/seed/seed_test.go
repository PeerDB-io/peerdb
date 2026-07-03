package seed

import (
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
