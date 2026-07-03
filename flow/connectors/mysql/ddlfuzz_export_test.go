//go:build ddlfuzz

package connmysql

import (
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
