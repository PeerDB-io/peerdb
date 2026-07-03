package compare

import (
	"errors"
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
			name: "multi statement truncation",
			our:  "alter t{drop c}",
			d: &digest.Digest{Verdict: "accept", Stmts: []digest.Stmt{
				{Kind: "alter_table", Table: "t", Specs: []digest.Spec{{Op: "drop", OldName: "c"}}},
				{Kind: "other"},
				{Kind: "alter_table", Table: "u", Specs: []digest.Spec{{Op: "drop", OldName: "d"}}},
			}},
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

func TestSplitTopLevel(t *testing.T) {
	got := SplitTopLevel([]byte("a,'b,c',fn(1,2),`d,e`"), ',')
	if len(got) != 4 {
		t.Fatalf("got %d parts: %q", len(got), got)
	}
}

func acceptAlter(specs ...digest.Spec) *digest.Digest {
	return &digest.Digest{Verdict: "accept", Stmts: []digest.Stmt{{Kind: "alter_table", Table: "t", Specs: specs}}}
}
