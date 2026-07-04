package connmysql

import (
	"encoding/json"
	"strconv"
	"testing"
)

func TestDDLFuzzExportParity(t *testing.T) {
	for i, tc := range tidbDiffModeCases {
		t.Run("mode/"+strconv.Itoa(i)+"/"+tc.note, func(t *testing.T) {
			assertFuzzParity(t, tc.sql, tc.sqlMode, tc.isMariaDB, tc.want)
		})
	}
	for i, tc := range tidbDiffCorpus {
		var runs []bool
		switch tc.engine {
		case "mysql":
			runs = []bool{false}
		case "mariadb":
			runs = []bool{true}
		default:
			runs = []bool{false, true}
		}
		for _, isMaria := range runs {
			t.Run("corpus/"+strconv.Itoa(i)+"/maria="+strconv.FormatBool(isMaria), func(t *testing.T) {
				assertFuzzParity(t, tc.sql, 0, isMaria, ddlDiffOurSig(tc.sql, 0, isMaria))
			})
		}
	}
}

func assertFuzzParity(t *testing.T, sql string, sqlMode uint64, isMariaDB bool, want string) {
	t.Helper()
	stmts, parseErr := parseQueryEvent([]byte(sql), sqlMode, isMariaDB)
	if parseErr == nil {
		if got, diff := fuzzDDLSignature(stmts), ddlDiffSignature(stmts); got != diff {
			t.Fatalf("fuzzDDLSignature=%q ddlDiffSignature=%q", got, diff)
		}
	}
	got, err := FuzzDDLSignature([]byte(sql), sqlMode, isMariaDB)
	if want == "ERROR" {
		if err == nil {
			t.Fatalf("FuzzDDLSignature succeeded: %q", got)
		}
		return
	}
	if err != nil {
		t.Fatalf("FuzzDDLSignature error: %v", err)
	}
	if got != want {
		t.Fatalf("FuzzDDLSignature=%q want %q", got, want)
	}
}

func TestDDLFuzzParseForE2EImplicitPositionShift(t *testing.T) {
	data, err := FuzzParseForE2E(
		[]byte("ALTER TABLE fixture RENAME COLUMN `new``tick` TO `имя2`, ADD COLUMN `new``tick` enum('a','b') NOT NULL"),
		0,
		false,
	)
	if err != nil {
		t.Fatalf("FuzzParseForE2E error: %v", err)
	}
	var got e2eStmts
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal e2e stmts: %v", err)
	}
	if len(got.Stmts) != 1 || len(got.Stmts[0].Specs) == 0 || !got.Stmts[0].Specs[0].HasPosition {
		t.Fatalf("expected implicit position shift in first spec: %s", data)
	}
}

func TestDDLFuzzParseForE2EAlterTableRename(t *testing.T) {
	data, err := FuzzParseForE2E([]byte("ALTER TABLE fixture RENAME t2"), 262191, false)
	if err != nil {
		t.Fatalf("FuzzParseForE2E error: %v", err)
	}
	var got e2eStmts
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal e2e stmts: %v", err)
	}
	if len(got.Stmts) != 1 || got.Stmts[0].Kind != "rename_table" || len(got.Stmts[0].Pairs) != 1 {
		t.Fatalf("expected one rename_table statement: %s", data)
	}
	pair := got.Stmts[0].Pairs[0]
	if pair.OldTable != "fixture" || pair.NewTable != "t2" {
		t.Fatalf("unexpected rename pair: %#v", pair)
	}
}

func TestDDLFuzzSignatureEscapesDelimiterIdentifiers(t *testing.T) {
	got, err := FuzzDDLSignature(
		[]byte("ALTER TABLE `mt5_managers` ADD COLUMN (`(` INT UNSIGNED NOT NULL DEFAULT 0,`B` INT UNSIGNED NOT NULL DEFAULT 0)"),
		0,
		true,
	)
	if err != nil {
		t.Fatalf("FuzzDDLSignature error: %v", err)
	}
	want := "alter mt5_managers{col `(`=uint32 nn, B=uint32 nn}"
	if got != want {
		t.Fatalf("FuzzDDLSignature=%q want %q", got, want)
	}
}
