package e2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRewriteCorpusStatement(t *testing.T) {
	live := snapshot{
		"id": {Name: "id", Ordinal: 1, ColumnType: "bigint", IsNullable: "NO", ColumnKey: "PRI"},
		"c":  {Name: "c", Ordinal: 2, ColumnType: "int", IsNullable: "YES"},
	}
	got, ok := rewriteCorpusStatement([]byte("ALTER TABLE oldtab ADD COLUMN newcol int"), false, live)
	if !ok {
		t.Fatal("rewrite rejected valid alter")
	}
	if !strings.Contains(got, "fixture") {
		t.Fatalf("rewrite = %q, want fixture table", got)
	}
	if strings.Contains(got, "oldtab") {
		t.Fatalf("rewrite retained old table: %q", got)
	}

	if _, ok := rewriteCorpusStatement([]byte("SELECT 1"), false, live); ok {
		t.Fatal("rewrite accepted non-actionable statement")
	}
	if replaced := replaceIdentOccurrences("xx_old old `old` old2", "old", "fixture"); replaced != "xx_old fixture `fixture` old2" {
		t.Fatalf("replaceIdentOccurrences = %q", replaced)
	}
}

func TestQueueLifecycle(t *testing.T) {
	dir := t.TempDir()
	if err := ensureStateLayout(dir); err != nil {
		t.Fatal(err)
	}
	pending := filepath.Join(dir, "e2e-queue", "pending", "abc123.json")
	if err := os.WriteFile(pending, []byte(`{"sig":"abc123","engine":"mysql","sql_mode_name":"","session_sql_mode":"","statement":"ALTER TABLE fixture ADD COLUMN n1 int"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	legacyPending := filepath.Join(dir, "e2e-queue", "pending", "legacy.json")
	if err := os.WriteFile(legacyPending, []byte(`{"sig":"legacy","engine":"mysql","sql_mode_name":"","statement":"ALTER TABLE fixture ADD COLUMN n2 int"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	items, err := claimQueueItems(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("items = %#v", items)
	}
	bySig := map[string]queueItem{}
	for _, item := range items {
		bySig[item.Sig] = item
	}
	item := bySig["abc123"]
	if item.Sig != "abc123" || item.Engine != EngineMySQL {
		t.Fatalf("item = %#v", item)
	}
	if item.SessionSQLMode == nil || *item.SessionSQLMode != "" {
		t.Fatalf("SessionSQLMode = %#v, want pointer to empty string", item.SessionSQLMode)
	}
	if legacy := bySig["legacy"]; legacy.SessionSQLMode != nil {
		t.Fatalf("legacy SessionSQLMode = %#v, want nil", legacy.SessionSQLMode)
	}
	if _, err := os.Stat(pending); !os.IsNotExist(err) {
		t.Fatalf("pending still exists or unexpected stat err: %v", err)
	}
	if err := completeQueueItem(dir, item, queueResult{Result: "confirmed-fixed"}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "e2e-queue", "done", "abc123.json")); err != nil {
		t.Fatalf("done missing: %v", err)
	}
	if _, err := os.Stat(item.path); !os.IsNotExist(err) {
		t.Fatalf("processing still exists or unexpected stat err: %v", err)
	}
}
