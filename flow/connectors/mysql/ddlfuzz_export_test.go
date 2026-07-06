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

func TestDDLFuzzParseForE2EModifyIfExistsIsConditionalChange(t *testing.T) {
	query := []byte("ALTER TABLE fixture MODIFY IF EXISTS `n1` INT UNSIGNED DEFAULT CURRENT_TIMESTAMP(6) NOT NULL")
	sig, err := FuzzDDLSignature(query, sqlModeNoBackslashEscapes, true)
	if err != nil {
		t.Fatalf("FuzzDDLSignature error: %v", err)
	}
	if sig != "alter fixture{col n1=uint32 nn}" {
		t.Fatalf("FuzzDDLSignature=%q", sig)
	}

	data, err := FuzzParseForE2E(query, sqlModeNoBackslashEscapes, true)
	if err != nil {
		t.Fatalf("FuzzParseForE2E error: %v", err)
	}
	var got e2eStmts
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal e2e stmts: %v", err)
	}
	if len(got.Stmts) != 1 || len(got.Stmts[0].Specs) != 1 {
		t.Fatalf("expected one alter spec: %s", data)
	}
	spec := got.Stmts[0].Specs[0]
	if spec.Op != "change" || spec.OldName != "n1" || len(spec.Cols) != 1 ||
		spec.Cols[0].Name != "n1" || spec.Cols[0].TypeStr != "int unsigned" || !spec.Cols[0].NotNull ||
		!spec.IfExists {
		t.Fatalf("unexpected e2e spec: %#v", spec)
	}
}

func TestDDLFuzzParseForE2EChangeIfExistsPreserved(t *testing.T) {
	query := []byte("ALTER TABLE `fixture` CHANGE IF EXISTS `fixture` `n3` BINARY(8) FIRST, FORCE, CHANGE `id` `n2` SERIAL FIRST")
	sig, err := FuzzDDLSignature(query, sqlModeNoBackslashEscapes, true)
	if err != nil {
		t.Fatalf("FuzzDDLSignature error: %v", err)
	}
	if sig != "alter fixture{chg fixture n3=bytes @pos; chg id n2=uint64 nn @pos}" {
		t.Fatalf("FuzzDDLSignature=%q", sig)
	}

	data, err := FuzzParseForE2E(query, sqlModeNoBackslashEscapes, true)
	if err != nil {
		t.Fatalf("FuzzParseForE2E error: %v", err)
	}
	var got e2eStmts
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal e2e stmts: %v", err)
	}
	if len(got.Stmts) != 1 || len(got.Stmts[0].Specs) != 2 {
		t.Fatalf("expected two alter specs: %s", data)
	}
	first := got.Stmts[0].Specs[0]
	if first.Op != "change" || first.OldName != "fixture" || len(first.Cols) != 1 ||
		first.Cols[0].Name != "n3" || first.Cols[0].TypeStr != "binary(8)" || !first.HasPosition ||
		!first.IfExists {
		t.Fatalf("unexpected first spec: %#v", first)
	}
	second := got.Stmts[0].Specs[1]
	if second.Op != "change" || second.OldName != "id" || len(second.Cols) != 1 ||
		second.Cols[0].Name != "n2" || second.Cols[0].TypeStr != "bigint unsigned" ||
		!second.Cols[0].NotNull || !second.HasPosition || second.IfExists {
		t.Fatalf("unexpected second spec: %#v", second)
	}
}

func TestDDLFuzzParseForE2EAddIfNotExistsPreserved(t *testing.T) {
	query := []byte("ALTER TABLE /*M! `fixture` */ ADD COLUMN IF NOT EXISTS `fixture` CHAR(8) NOT NULL")
	data, err := FuzzParseForE2E(query, sqlModeANSIQuotes|sqlModeNoBackslashEscapes, true)
	if err != nil {
		t.Fatalf("FuzzParseForE2E error: %v", err)
	}
	var got e2eStmts
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal e2e stmts: %v", err)
	}
	if len(got.Stmts) != 1 || len(got.Stmts[0].Specs) != 1 {
		t.Fatalf("expected one alter spec: %s", data)
	}
	spec := got.Stmts[0].Specs[0]
	if spec.Op != "add" || !spec.IfNotExists || len(spec.Cols) != 1 ||
		spec.Cols[0].Name != "fixture" || spec.Cols[0].TypeStr != "char(8)" || !spec.Cols[0].NotNull {
		t.Fatalf("unexpected e2e spec: %#v", spec)
	}
}

func TestDDLFuzzParseForE2ESuppressesDuplicateConditionalAdd(t *testing.T) {
	query := []byte("ALTER TABLE fixture ADD (n1 MIDDLEINT, n2 BINARY, INDEX (after)), ADD COLUMN IF NOT EXISTS n1 LINESTRING")
	sig, err := FuzzDDLSignature(query, 0, true)
	if err != nil {
		t.Fatalf("FuzzDDLSignature error: %v", err)
	}
	if sig != "alter fixture{col n1=int32, n2=bytes; col n1=geometry}" {
		t.Fatalf("FuzzDDLSignature=%q", sig)
	}

	data, err := FuzzParseForE2E(query, 0, true)
	if err != nil {
		t.Fatalf("FuzzParseForE2E error: %v", err)
	}
	var got e2eStmts
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal e2e stmts: %v", err)
	}
	if len(got.Stmts) != 1 || len(got.Stmts[0].Specs) != 1 || len(got.Stmts[0].Specs[0].Cols) != 2 {
		t.Fatalf("expected duplicate conditional add to be suppressed: %s", data)
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
