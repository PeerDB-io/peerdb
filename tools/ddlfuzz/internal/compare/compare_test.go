package compare

import (
	"errors"
	"strings"
	"testing"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/digest"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/run"
)

func TestDiffReconciliationRules(t *testing.T) {
	base := run.Case{SQL: []byte("ALTER TABLE t ADD c INT"), Engine: run.EngineMySQL}
	tests := []struct {
		name string
		our  string
		err  error
		d    *digest.Digest
		want string
	}{
		{
			name: "reject reconciles",
			err:  errors.New("anything"),
			d:    &digest.Digest{Verdict: "reject", Error: "server"},
		},
		{
			name: "numeric defaults",
			our:  "alter t{col c=numeric(-1,-1)}",
			d: acceptAlter(digest.Spec{Op: "add", Cols: []digest.Col{{
				Name: "c", TypeStr: "decimal(10,0)", ParamsWritten: []int{10, 0},
			}}}),
		},
		{
			name: "int display width",
			our:  "alter t{col c=int32}",
			d: acceptAlter(digest.Spec{Op: "add", Cols: []digest.Col{{
				Name: "c", TypeStr: "int(11)",
			}}}),
		},
		{
			name: "bool tinyint one",
			our:  "alter t{col c=bool}",
			d: acceptAlter(digest.Spec{Op: "add", Cols: []digest.Col{{
				Name: "c", TypeStr: "tinyint(1) unsigned",
			}}}),
		},
		{
			name: "unknown type ERR",
			our:  "alter t{col c=ERR}",
			d: acceptAlter(digest.Spec{Op: "add", Cols: []digest.Col{{
				Name: "c", TypeStr: "uuid",
			}}}),
		},
		{
			name: "flatten change same name order position",
			our:  "alter t{drop d; ren a>b; col c=int32, e=string @pos}",
			d: &digest.Digest{Verdict: "accept", Stmts: []digest.Stmt{{
				Kind: "alter_table", Table: "t", Specs: []digest.Spec{
					{Op: "change", OldName: "c", Cols: []digest.Col{{Name: "c", TypeStr: "int"}}},
					{Op: "modify", Cols: []digest.Col{{Name: "e", TypeStr: "varchar(10)"}}, HasPosition: true},
					{Op: "rename_col", OldName: "a", NewName: "b"},
					{Op: "drop", OldName: "d"},
				},
			}}},
		},
		{
			name: "raw backtick in identifier with reordered specs",
			our:  "alter fixture{ren new`tick>имя2; col new`tick=enum nn}",
			d: &digest.Digest{Verdict: "accept", Stmts: []digest.Stmt{{
				Kind: "alter_table", Table: "fixture", Specs: []digest.Spec{
					{Op: "add", Cols: []digest.Col{{Name: "new`tick", TypeStr: "enum", NotNull: true}}},
					{Op: "rename_col", OldName: "new`tick", NewName: "имя2"},
				},
			}}},
		},
		{
			name: "multi statement truncation",
			our:  "alter t{drop c}",
			d: &digest.Digest{Verdict: "accept", Stmts: []digest.Stmt{
				{Kind: "alter_table", Table: "t", Specs: []digest.Spec{{Op: "drop", OldName: "c"}}},
				{Kind: "other"},
				{Kind: "alter_table", Table: "u", Specs: []digest.Spec{{Op: "drop", OldName: "d"}}},
			}},
		},
		{
			name: "escaped comma identifier reconciles",
			our:  "alter t{col `firs,`=int32 @pos}",
			d: acceptAlter(digest.Spec{Op: "add", Cols: []digest.Col{{
				Name: "firs,", TypeStr: "int",
			}}, HasPosition: true}),
		},
		{
			name: "escaped empty identifier reconciles",
			our:  "alter t{drop ``}",
			d:    acceptAlter(digest.Spec{Op: "drop", OldName: ""}),
		},
		{
			name: "escaped paren identifier parenthesized add flattens",
			our:  "alter mt5_managers{col `(`=uint32 nn, B=uint32 nn}",
			d: &digest.Digest{Verdict: "accept", Stmts: []digest.Stmt{{
				Kind: "alter_table", Table: "mt5_managers", Specs: []digest.Spec{
					{Op: "add", Cols: []digest.Col{{Name: "(", TypeStr: "int(10) unsigned", NotNull: true, ParamsWritten: []int{10}}}},
					{Op: "add", Cols: []digest.Col{{Name: "B", TypeStr: "int(10) unsigned", NotNull: true, ParamsWritten: []int{10}}}},
				},
			}}},
		},
		{
			name: "alter table rename only reconciles as rename",
			our:  "rename t>t2",
			d: &digest.Digest{Verdict: "accept", Stmts: []digest.Stmt{{
				Kind: "alter_table", Table: "t", NewTable: "t2",
			}}},
		},
		{
			name: "alter table rename keeps schema as written",
			our:  "rename d1.t>d1.t2",
			d: &digest.Digest{Verdict: "accept", Stmts: []digest.Stmt{{
				Kind: "alter_table", Schema: "d1", Table: "t", NewSchema: "d1", NewTable: "t2",
			}}},
		},
		{
			name: "alter table cross schema rename",
			our:  "rename d1.t>d2.t2",
			d: &digest.Digest{Verdict: "accept", Stmts: []digest.Stmt{{
				Kind: "alter_table", Schema: "d1", Table: "t", NewSchema: "d2", NewTable: "t2",
			}}},
		},
		{
			name: "alter table rename to same name is a noop",
			our:  "alter t{}",
			d: &digest.Digest{Verdict: "accept", Stmts: []digest.Stmt{{
				Kind: "alter_table", Table: "t", NewTable: "t",
			}}},
		},
		{
			name: "alter table rename with specs splits into alter and rename",
			our:  "alter t{col c=int32} | rename t>t2",
			d: &digest.Digest{Verdict: "accept", Stmts: []digest.Stmt{{
				Kind: "alter_table", Table: "t", NewTable: "t2",
				Specs: []digest.Spec{{Op: "add", Cols: []digest.Col{{Name: "c", TypeStr: "int"}}}},
			}}},
		},
		{
			name: "alter table rename does not mask spec mismatch",
			our:  "alter t{col c=int64} | rename t>t2",
			d: &digest.Digest{Verdict: "accept", Stmts: []digest.Stmt{{
				Kind: "alter_table", Table: "t", NewTable: "t2",
				Specs: []digest.Spec{{Op: "add", Cols: []digest.Col{{Name: "c", TypeStr: "int"}}}},
			}}},
			want: "sig_mismatch",
		},
		{
			name: "alter table rename does not mask target mismatch",
			our:  "rename t>t3",
			d: &digest.Digest{Verdict: "accept", Stmts: []digest.Stmt{{
				Kind: "alter_table", Table: "t", NewTable: "t2",
			}}},
			want: "sig_mismatch",
		},
		{
			name: "mismatch shape",
			our:  "alter t{col c=int32}",
			d: acceptAlter(digest.Spec{Op: "add", Cols: []digest.Col{{
				Name: "c", TypeStr: "smallint",
			}}}),
			want: "sig_mismatch",
		},
		{
			name: "we error",
			err:  errors.New(`parse "abc" at byte 12`),
			d:    acceptAlter(digest.Spec{Op: "drop", OldName: "c"}),
			want: "we_error",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			div := Diff(base, tc.our, tc.err, nil, tc.d)
			if tc.want == "" && div != nil {
				t.Fatalf("unexpected divergence: %+v", div)
			}
			if tc.want != "" {
				if div == nil {
					t.Fatalf("expected divergence %s", tc.want)
				}
				if div.Class != tc.want {
					t.Fatalf("class got %s want %s", div.Class, tc.want)
				}
			}
		})
	}
}

func TestFirstDifferenceEnrichedShapes(t *testing.T) {
	tests := []struct {
		name   string
		ours   string
		theirs string
		want   string
	}{
		{
			name:   "stmt count kind sequence",
			ours:   "alter t{col a=int32} | rename t>u",
			theirs: "alter t{col a=int32}",
			want:   "stmt_count(alter+rename≠alter)",
		},
		{
			name:   "stmt count capped",
			ours:   "alter t{col a=int32} | rename t>u | alter u{drop a} | rename u>v | alter v{col b=int32} | rename v>w",
			theirs: "alter t{col a=int32}",
			want:   "stmt_count(alter+rename+alter+rename+alter+…≠alter)",
		},
		{
			name:   "spec count kind sequence",
			ours:   "alter t{col a=int32; ren b>c; drop d}",
			theirs: "alter t{col a=int32; drop d}",
			want:   "spec_count(3,2;col+ren+drop≠col+drop)",
		},
		{
			name:   "spec count capped",
			ours:   "alter t{col a=int32; col b=int32; col c=int32; col d=int32; col e=int32; col f=int32; col g=int32}",
			theirs: "alter t{col a=int32}",
			want:   "spec_count(7,1;col+col+col+col+col+col+…≠col)",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := FirstDifference(tc.ours, tc.theirs); got != tc.want {
				t.Fatalf("FirstDifference()=%q, want %q", got, tc.want)
			}
		})
	}
}

func TestFirstDifferenceIdentifierFreeShapes(t *testing.T) {
	a := FirstDifference("alter `db one`.`tab one`{col `secret_a`=int32} | rename `tab one`>`tab two`", "alter `other`.`x`{col `secret_b`=int32}")
	b := FirstDifference("alter `db two`.`tab three`{col `leak_me`=int32} | rename `tab three`>`tab four`", "alter `more`.`y`{col `another`=int32}")
	if a != b {
		t.Fatalf("identifier-only changes altered shape: %q vs %q", a, b)
	}
	if strings.Contains(a, "secret") || strings.Contains(a, "tab") || strings.Contains(a, "db") {
		t.Fatalf("shape leaks identifier text: %q", a)
	}
}

func TestDiffParseShapesIncludeMaskedReason(t *testing.T) {
	base := run.Case{SQL: []byte("ALTER TABLE t ADD c INT"), Engine: run.EngineMySQL}
	our := Diff(base, "bogus secret_table", nil, nil, acceptAlter(digest.Spec{Op: "add", Cols: []digest.Col{{Name: "c", TypeStr: "int"}}}))
	if our == nil || our.Shape != `parse_our_sig(unknown statement signature "?")` {
		t.Fatalf("parse_our_sig shape = %#v", our)
	}
	oracle := Diff(base, "alter t{}", nil, nil, &digest.Digest{Verdict: "accept", Stmts: []digest.Stmt{{Kind: "rename_table"}}})
	if oracle == nil || oracle.Shape != `parse_oracle_sig(unknown statement signature "?")` {
		t.Fatalf("parse_oracle_sig shape = %#v", oracle)
	}
}

func TestColumnShapePayloadsDoNotLeakIdentifiers(t *testing.T) {
	if got := sanitizeKind("uint32, B=uint32; col B=uint32"); got != "uint32" {
		t.Fatalf("sanitizeKind leak regression got %q", got)
	}
	got := FirstDifference("alter t{col a=numeric(10,2)}", "alter t{col a=numeric(11,3)}")
	if got != "col_params(10,2≠11,3)" {
		t.Fatalf("col_params shape=%q", got)
	}
	if strings.Contains(got, " a") || strings.Contains(got, " b") || strings.Contains(got, "=a") || strings.Contains(got, "=b") {
		t.Fatalf("col_params leaked identifier text: %q", got)
	}
}

func TestContainsSkippedVersionComment(t *testing.T) {
	tests := []struct {
		sql    string
		engine uint8
		want   bool
	}{
		{"ALTER TABLE t /*!80000 ADD COLUMN c INT */ DROP COLUMN d", run.EngineMariaDB, true},
		{"ALTER TABLE t /*!80000 ADD COLUMN c INT */", run.EngineMySQL, false},
		{"ALTER TABLE t /*!50600 ADD COLUMN c INT */", run.EngineMariaDB, false},
		{"ALTER TABLE t /*!130200 ADD COLUMN c INT */", run.EngineMariaDB, true},
		{"ALTER TABLE t /*!100500 ADD COLUMN c INT */", run.EngineMariaDB, false},
		{"ALTER TABLE t /*!90700 ADD COLUMN c INT */", run.EngineMySQL, false},
		{"ALTER TABLE t /*!90701 ADD COLUMN c INT */", run.EngineMySQL, true},
		// MySQL only takes the 6th digit when whitespace follows it
		{"ALTER TABLE t /*!123456 ADD COLUMN c INT */", run.EngineMySQL, true},
		{"ALTER TABLE t /*!888888x*/", run.EngineMySQL, false},
		{"ALTER TABLE t /*M!140000 ADD COLUMN c INT */", run.EngineMariaDB, true},
		{"ALTER TABLE t /*M!100500 ADD COLUMN c INT */", run.EngineMariaDB, false},
		{"ALTER TABLE t /*M!100500 ADD COLUMN c INT */", run.EngineMySQL, false},
		// reversed comments never keep their digits in binlogged text
		{"ALTER TABLE t /*!!100500 ADD COLUMN c INT */", run.EngineMariaDB, true},
		{"ALTER TABLE t /*!! ADD COLUMN c INT */", run.EngineMariaDB, false},
		{"ALTER TABLE t /*! ADD COLUMN c INT */", run.EngineMariaDB, false},
		{"ALTER TABLE t /*!123 ADD COLUMN c INT */", run.EngineMySQL, false},
		{"ALTER TABLE t ADD c INT", run.EngineMySQL, false},
	}
	for _, tc := range tests {
		if got := ContainsSkippedVersionComment([]byte(tc.sql), tc.engine); got != tc.want {
			t.Errorf("ContainsSkippedVersionComment(%q, %d) = %v, want %v", tc.sql, tc.engine, got, tc.want)
		}
	}
}

func TestDescriptorStability(t *testing.T) {
	a := Descriptor{V: 1, Engine: "mysql", SQLMode: SQLModeANSIQuotes | 1<<63, Class: "we_error", Lane: "fast", Shape: NormalizeError(`parse "table_a" at byte 10`)}
	b := Descriptor{V: 1, Engine: "mysql", SQLMode: SQLModeANSIQuotes, Class: "we_error", Lane: "fast", Shape: NormalizeError(`parse "table_b" at byte 999`)}
	if DescriptorSig(a) != DescriptorSig(b) {
		t.Fatalf("descriptor not stable: %s != %s", DescriptorSig(a), DescriptorSig(b))
	}
	b.Engine = "mariadb"
	if DescriptorSig(a) == DescriptorSig(b) {
		t.Fatalf("engine should affect descriptor")
	}
	b.Engine = "mysql"
	b.SQLMode = SQLModeNoBackslashEscapes
	if DescriptorSig(a) == DescriptorSig(b) {
		t.Fatalf("masked mode should affect descriptor")
	}
}

func TestDescriptorSigPinnedUnaffectedShapes(t *testing.T) {
	tests := []struct {
		name string
		desc Descriptor
		want string
	}{
		{"col_kind", Descriptor{V: 1, Engine: "mysql", Class: "sig_mismatch", Lane: "fast", Shape: "col_kind(int32≠string)"}, "1458dcc640e4"},
		{"table_qual", Descriptor{V: 1, Engine: "mysql", Class: "sig_mismatch", Lane: "fast", Shape: "table_qual"}, "b71499ab904f"},
		{"we_error", Descriptor{V: 1, Engine: "mysql", SQLMode: SQLModeANSIQuotes, Class: "we_error", Lane: "fast", Shape: `parse "?" at byte ?`}, "5182d84c4d34"},
	}
	for _, tc := range tests {
		if got := DescriptorSig(tc.desc); got != tc.want {
			t.Fatalf("%s DescriptorSig=%s, want %s", tc.name, got, tc.want)
		}
	}
}

func TestStmtCountPathologyDistinctDescriptorSigs(t *testing.T) {
	base := Descriptor{V: 1, Engine: "mysql", Class: "sig_mismatch", Lane: "fast"}
	a := base
	a.Shape = FirstDifference("alter t{col a=int32} | rename t>u", "alter t{col a=int32}")
	b := base
	b.Shape = FirstDifference("alter t{col a=int32} | alter u{drop b}", "alter t{col a=int32}")
	if a.Shape == b.Shape {
		t.Fatalf("pathology shapes still collapse: %q", a.Shape)
	}
	if DescriptorSig(a) == DescriptorSig(b) {
		t.Fatalf("descriptor sigs collapsed for %q and %q", a.Shape, b.Shape)
	}
}

func TestSplitTopLevel(t *testing.T) {
	got := SplitTopLevel([]byte("a,'b,c',fn(1,2),`d,e`"), ',')
	if len(got) != 4 {
		t.Fatalf("got %d parts: %q", len(got), got)
	}
}

func acceptAlter(specs ...digest.Spec) *digest.Digest {
	return &digest.Digest{Verdict: "accept", Stmts: []digest.Stmt{{Kind: "alter_table", Table: "t", Specs: specs}}}
}
