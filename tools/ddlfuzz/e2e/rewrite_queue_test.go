package e2e

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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

func TestProcessingQueueCleanupRejectsMySQLMariaDBOnlySQLMode(t *testing.T) {
	dir := t.TempDir()
	if err := ensureStateLayout(dir); err != nil {
		t.Fatal(err)
	}
	body := `{"sig":"fresh","engine":"mysql","session_sql_mode":"MSSQL,NO_BACKSLASH_ESCAPES","statement":"ALTER TABLE fixture ADD COLUMN n1 int"}`
	if err := os.WriteFile(filepath.Join(dir, "e2e-queue", "processing", "fresh.json"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := requeueStaleProcessing(dir, time.Hour); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "e2e-queue", "done", "fresh.json"))
	if err != nil {
		t.Fatalf("done missing: %v", err)
	}
	var result queueResult
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	if result.Result != "exec-reject" || !strings.Contains(result.Details, "MSSQL") {
		t.Fatalf("result = %#v, want exec-reject mentioning MSSQL", result)
	}
	if _, err := os.Stat(filepath.Join(dir, "e2e-queue", "processing", "fresh.json")); !os.IsNotExist(err) {
		t.Fatalf("processing still exists or unexpected stat err: %v", err)
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

func TestQueueClaimRejectsMySQLMariaDBOnlySQLMode(t *testing.T) {
	dir := t.TempDir()
	if err := ensureStateLayout(dir); err != nil {
		t.Fatal(err)
	}
	pending := filepath.Join(dir, "e2e-queue", "pending")
	writes := map[string]string{
		"oracle": `{"sig":"oracle","engine":"mysql","session_sql_mode":"ORACLE,NO_BACKSLASH_ESCAPES","statement":"ALTER TABLE fixture ADD COLUMN n1 int"}`,
		"mssql":  fmt.Sprintf(`{"sig":"mssql","engine":"mysql","sql_mode":%d,"statement":"ALTER TABLE fixture ADD COLUMN n2 int"}`, sqlModeMSSQL),
		"maria":  `{"sig":"maria","engine":"mariadb","session_sql_mode":"ORACLE,NO_BACKSLASH_ESCAPES","statement":"ALTER TABLE fixture ADD COLUMN n3 int"}`,
	}
	for sig, body := range writes {
		if err := os.WriteFile(filepath.Join(pending, sig+".json"), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	items, err := claimQueueItems(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Sig != "maria" || items[0].Engine != EngineMariaDB {
		t.Fatalf("items = %#v, want only compatible mariadb item", items)
	}
	for _, tc := range []struct {
		sig   string
		token string
	}{
		{sig: "oracle", token: "ORACLE"},
		{sig: "mssql", token: "MSSQL"},
	} {
		data, err := os.ReadFile(filepath.Join(dir, "e2e-queue", "done", tc.sig+".json"))
		if err != nil {
			t.Fatalf("%s done missing: %v", tc.sig, err)
		}
		var result queueResult
		if err := json.Unmarshal(data, &result); err != nil {
			t.Fatal(err)
		}
		if result.Result != "exec-reject" || !strings.Contains(result.Details, tc.token) {
			t.Fatalf("%s result = %#v, want exec-reject mentioning %s", tc.sig, result, tc.token)
		}
		if _, err := os.Stat(filepath.Join(dir, "e2e-queue", "processing", tc.sig+".json")); !os.IsNotExist(err) {
			t.Fatalf("%s processing still exists or unexpected stat err: %v", tc.sig, err)
		}
	}
}
