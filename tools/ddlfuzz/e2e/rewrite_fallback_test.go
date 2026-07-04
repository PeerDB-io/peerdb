package e2e

import (
	"regexp"
	"slices"
	"strings"
	"testing"
)

func TestRewriteCorpusStatementFreshAndRenameTargets(t *testing.T) {
	live := testSnapshot("id", "c_int", "n1")
	got, ok := rewriteCorpusStatement([]byte("ALTER TABLE oldtab ADD COLUMN n1 int"), false, live)
	if !ok {
		t.Fatal("rewrite rejected valid add")
	}
	if strings.Contains(got, "oldtab") || !strings.Contains(got, "fixture") {
		t.Fatalf("rewrite table target = %q, want fixture", got)
	}
	if strings.Contains(got, "ADD COLUMN n1") || strings.Contains(got, "`n1`") {
		t.Fatalf("rewrite retained live fresh name: %q", got)
	}
	if !strings.Contains(got, "ADD COLUMN n2") {
		t.Fatalf("rewrite = %q, want first free palette name n2", got)
	}

	allFreshLive := testSnapshot(append([]string{"id", "c_int"}, FreshNames...)...)
	got, ok = rewriteCorpusStatement([]byte("ALTER TABLE oldtab ADD (new_a int, new_b int)"), false, allFreshLive)
	if !ok {
		t.Fatal("rewrite rejected valid add tuple")
	}
	re := regexp.MustCompile(`ADD \((` + rewriteIdentPattern + `) int, (` + rewriteIdentPattern + `) int\)`)
	m := re.FindStringSubmatch(got)
	if m == nil {
		t.Fatalf("rewrite = %q, want add tuple", got)
	}
	left, right := unquoteRewriteIdent(m[1]), unquoteRewriteIdent(m[2])
	if left == right {
		t.Fatalf("synthesized rewrite names were not distinct: %q", got)
	}
	liveSet := make(map[string]bool, len(allFreshLive))
	for name := range allFreshLive {
		liveSet[name] = true
	}
	if liveSet[left] || liveSet[right] {
		t.Fatalf("synthesized rewrite used live name(s) %q/%q in %q", left, right, got)
	}

	got, ok = rewriteCorpusStatement([]byte("RENAME TABLE t1 TO t2"), false, testSnapshot("id"))
	if !ok {
		t.Fatal("rewrite rejected valid rename")
	}
	if got != "RENAME TABLE fixture TO fixture_r_c0" {
		t.Fatalf("rename rewrite = %q", got)
	}

	if got, ok := rewriteCorpusStatement([]byte("RENAME TABLE t TO t"), false, testSnapshot("id")); ok {
		t.Fatalf("self-rename accepted: %q", got)
	}
}

func TestSimpleFallbackDDLForModeQuotesHostileNames(t *testing.T) {
	tests := []struct {
		name      string
		seq       uint64
		modeEntry string
		want      string
		bare      string
	}{
		{"oracle backtick", 13, "ORACLE", `"new` + "`" + `tick"`, "new`tick"},
		{"mssql space", 14, "MSSQL", `"spelled name"`, "spelled name"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := simpleFallbackDDLForMode(tt.seq, snapshot{}, tt.modeEntry)
			if !strings.Contains(got, tt.want) {
				t.Fatalf("fallback = %q, want quoted %s", got, tt.want)
			}
			if strings.Contains(got, "ADD COLUMN "+tt.bare+" int") {
				t.Fatalf("fallback used bare hostile name: %q", got)
			}
		})
	}

	gotMode := simpleFallbackDDLForMode(13, snapshot{}, "ANSI_QUOTES")
	gotPlain := simpleFallbackDDL(13, snapshot{})
	if gotMode != gotPlain {
		t.Fatalf("non Oracle/MSSQL fallback = %q, want %q", gotMode, gotPlain)
	}
}

func TestSimpleFallbackDDLAfterTargetUsesSnapshot(t *testing.T) {
	got := simpleFallbackDDL(6, testSnapshot("id", "c_int"))
	if !strings.Contains(got, " AFTER `id`") {
		t.Fatalf("fallback = %q, want AFTER snapshot column", got)
	}
	if strings.Contains(got, " AFTER `first`") {
		t.Fatalf("fallback used missing first column: %q", got)
	}

	got = simpleFallbackDDL(6, snapshot{})
	if strings.Contains(got, " AFTER ") {
		t.Fatalf("empty snapshot fallback has AFTER suffix: %q", got)
	}

	got = simpleFallbackDDL(6, testSnapshot("id", "first"))
	if !strings.HasSuffix(got, " AFTER `first`") {
		t.Fatalf("fallback = %q, want exact first suffix", got)
	}
}

func testSnapshot(names ...string) snapshot {
	out := make(snapshot, len(names))
	for i, name := range names {
		out[name] = colRow{Name: name, Ordinal: i + 1, ColumnType: "int", IsNullable: "YES"}
	}
	return out
}

const rewriteIdentPattern = "(?:`(?:``|[^`])*`|\"(?:\"\"|[^\"])*\"|\\[(?:\\]\\]|[^\\]])*\\]|[[:alnum:]_]+)"

func unquoteRewriteIdent(s string) string {
	if strings.HasPrefix(s, "`") && strings.HasSuffix(s, "`") {
		return strings.ReplaceAll(s[1:len(s)-1], "``", "`")
	}
	if strings.HasPrefix(s, `"`) && strings.HasSuffix(s, `"`) {
		return strings.ReplaceAll(s[1:len(s)-1], `""`, `"`)
	}
	if strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]") {
		return strings.ReplaceAll(s[1:len(s)-1], "]]", "]")
	}
	return s
}

func TestFreshNamesFixtureHasHostileEntriesAtExpectedIndices(t *testing.T) {
	if got := FreshNames[13]; got != "new`tick" {
		t.Fatalf("FreshNames[13] = %q", got)
	}
	if got := FreshNames[14]; got != "spelled name" {
		t.Fatalf("FreshNames[14] = %q", got)
	}
	if !slices.Contains(FreshNames, "имя2") {
		t.Fatal("FreshNames missing utf8 hostile entry")
	}
}
