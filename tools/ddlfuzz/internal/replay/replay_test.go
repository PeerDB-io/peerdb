package replay

import "testing"

func TestOfflineE2EReproducible(t *testing.T) {
	complete := map[string]any{
		"lane":              "e2e",
		"class":             "e2e-col-attr",
		"submitted_text":    "ALTER TABLE t ADD COLUMN n1 INT",
		"binlog_query":      "ALTER TABLE t ADD COLUMN n1 INT",
		"status_vars_hex":   "00",
		"before_snapshot":   map[string]any{},
		"after_snapshot":    map[string]any{},
		"info_schema_delta": map[string]any{},
	}
	if !offlineE2EReproducible(complete) {
		t.Fatal("complete e2e capture meta should be offline-reproducible")
	}

	legacy := map[string]any{
		"lane":            "e2e",
		"class":           "e2e-position-missed",
		"submitted_text":  "ALTER TABLE t ADD COLUMN n1 INT",
		"status_vars_hex": "00",
	}
	if offlineE2EReproducible(legacy) {
		t.Fatal("pre-capture e2e meta must fall back to the fast lane")
	}

	oracleReject := map[string]any{
		"lane":  "e2e",
		"class": "oracle-reject-live-accept",
	}
	if offlineE2EReproducible(oracleReject) {
		t.Fatal("oracle-reject-live-accept must fall back to the fast lane")
	}

	fast := map[string]any{"lane": "fast", "class": "sig_mismatch"}
	if offlineE2EReproducible(fast) {
		t.Fatal("fast-lane finding must never take the e2e path")
	}
}
