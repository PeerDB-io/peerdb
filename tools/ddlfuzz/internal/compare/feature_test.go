package compare

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/digest"
	ddllexec "github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/exec"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/run"
)

var featCase = run.Case{SQL: []byte("ALTER TABLE t ADD c INT"), Engine: run.EngineMySQL, SQLMode: SQLModeANSIQuotes}

func feats(t *testing.T, c run.Case, sig string, err error, p *ddllexec.PanicInfo, d *digest.Digest) string {
	t.Helper()
	a := BehaviorFeatures(c, sig, err, p, d, nil)
	b := BehaviorFeatures(c, sig, err, p, d, nil)
	slices.Sort(a)
	slices.Sort(b)
	sa, sb := fmt.Sprintf("%x", a), fmt.Sprintf("%x", b)
	if sa != sb {
		t.Fatalf("features unstable for identical inputs: %s != %s", sa, sb)
	}
	return sa
}

// The pinned values guard cross-process stability: the virgin bitmap persists
// across restarts, so any change to the coarse tuple or hash invalidates
// persisted bitmaps and must be a conscious diff (and reset the saved bitmaps).
func TestBehaviorFeaturesPinned(t *testing.T) {
	d := acceptAlter(digest.Spec{Op: "add", Cols: []digest.Col{{Name: "c", TypeStr: "int"}}})
	got := BehaviorFeatures(featCase, "alter t{col c=int32}", nil, nil, d, nil)
	want := []uint64{0x6dd40fecc232f8fb, 0x70fa83f5f803d19a}
	if !slices.Equal(got, want) {
		t.Fatalf("BehaviorFeatures pinned values drifted: got %#x, want %#x (bitmap-invalidating change?)", got, want)
	}
}

func TestBehaviorFeaturesBufReuse(t *testing.T) {
	d := acceptAlter(digest.Spec{Op: "add", Cols: []digest.Col{{Name: "c", TypeStr: "int"}}})
	buf := make([]uint64, 0, 16)
	a := BehaviorFeatures(featCase, "alter t{col c=int32}", nil, nil, d, buf)
	b := BehaviorFeatures(featCase, "alter t{col c=int32}", nil, nil, d, a[:0])
	if !slices.Equal(a, b) {
		t.Fatalf("buffer reuse changed features: %#x vs %#x", a, b)
	}
}

func TestBehaviorFeaturesIdentifierAndParamInvariance(t *testing.T) {
	tests := []struct {
		name string
		sigA string
		dA   *digest.Digest
		sigB string
		dB   *digest.Digest
	}{
		{
			name: "identifiers",
			sigA: "alter secret{col secret_col=int32}",
			dA:   acceptAlter(digest.Spec{Op: "add", Cols: []digest.Col{{Name: "secret_col", TypeStr: "int"}}}),
			sigB: "alter other{col other_col=int32}",
			dB:   acceptAlter(digest.Spec{Op: "add", Cols: []digest.Col{{Name: "other_col", TypeStr: "int"}}}),
		},
		{
			name: "quoted identifiers",
			sigA: "alter `a b`{drop `x y`}",
			dA:   acceptAlter(digest.Spec{Op: "drop", OldName: "x y"}),
			sigB: "alter `c;d`{drop `p{q`}",
			dB:   acceptAlter(digest.Spec{Op: "drop", OldName: "p{q"}),
		},
		{
			name: "type params",
			sigA: "alter t{col a=numeric(10,2)}",
			dA:   acceptAlter(digest.Spec{Op: "add", Cols: []digest.Col{{Name: "a", TypeStr: "decimal(10,2)"}}}),
			sigB: "alter t{col b=numeric(38,0)}",
			dB:   acceptAlter(digest.Spec{Op: "add", Cols: []digest.Col{{Name: "b", TypeStr: "decimal(38,0)"}}}),
		},
		{
			name: "spec order",
			sigA: "alter t{col a=int32; drop b}",
			dA: acceptAlter(
				digest.Spec{Op: "add", Cols: []digest.Col{{Name: "a", TypeStr: "int"}}},
				digest.Spec{Op: "drop", OldName: "b"},
			),
			sigB: "alter t{drop b; col a=int32}",
			dB: acceptAlter(
				digest.Spec{Op: "drop", OldName: "b"},
				digest.Spec{Op: "add", Cols: []digest.Col{{Name: "a", TypeStr: "int"}}},
			),
		},
		{
			name: "count bucket 3 vs 5",
			sigA: "alter t{col a=int32; col b=int32; col c=int32}",
			dA: acceptAlter(
				digest.Spec{Op: "add", Cols: []digest.Col{{Name: "a", TypeStr: "int"}}},
				digest.Spec{Op: "add", Cols: []digest.Col{{Name: "b", TypeStr: "int"}}},
				digest.Spec{Op: "add", Cols: []digest.Col{{Name: "c", TypeStr: "int"}}},
			),
			sigB: "alter t{col a=int32; col b=int32; col c=int32; col d=int32; col e=int32}",
			dB: acceptAlter(
				digest.Spec{Op: "add", Cols: []digest.Col{{Name: "a", TypeStr: "int"}}},
				digest.Spec{Op: "add", Cols: []digest.Col{{Name: "b", TypeStr: "int"}}},
				digest.Spec{Op: "add", Cols: []digest.Col{{Name: "c", TypeStr: "int"}}},
				digest.Spec{Op: "add", Cols: []digest.Col{{Name: "d", TypeStr: "int"}}},
				digest.Spec{Op: "add", Cols: []digest.Col{{Name: "e", TypeStr: "int"}}},
			),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := feats(t, featCase, tc.sigA, nil, nil, tc.dA)
			b := feats(t, featCase, tc.sigB, nil, nil, tc.dB)
			if a != b {
				t.Fatalf("%s variation changed features:\n%s\n%s", tc.name, a, b)
			}
		})
	}
}

func TestBehaviorFeaturesStructuralSensitivity(t *testing.T) {
	base := acceptAlter(digest.Spec{Op: "add", Cols: []digest.Col{{Name: "c", TypeStr: "int"}}})
	baseFeats := feats(t, featCase, "alter t{col c=int32}", nil, nil, base)
	distinct := map[string]string{
		"spec kind": feats(t, featCase, "alter t{drop c}", nil, nil,
			acceptAlter(digest.Spec{Op: "drop", OldName: "c"})),
		"type family": feats(t, featCase, "alter t{col c=string}", nil, nil,
			acceptAlter(digest.Spec{Op: "add", Cols: []digest.Col{{Name: "c", TypeStr: "text"}}})),
		"count bucket 1 vs 2": feats(t, featCase, "alter t{col c=int32; col d=int32}", nil, nil,
			acceptAlter(
				digest.Spec{Op: "add", Cols: []digest.Col{{Name: "c", TypeStr: "int"}}},
				digest.Spec{Op: "add", Cols: []digest.Col{{Name: "d", TypeStr: "int"}}},
			)),
		"qualification": feats(t, featCase, "alter db.t{col c=int32}", nil, nil,
			&digest.Digest{Verdict: "accept", Stmts: []digest.Stmt{{Kind: "alter_table", Schema: "db", Table: "t", Specs: []digest.Spec{{Op: "add", Cols: []digest.Col{{Name: "c", TypeStr: "int"}}}}}}}),
		"position": feats(t, featCase, "alter t{col c=int32 @pos}", nil, nil,
			acceptAlter(digest.Spec{Op: "add", HasPosition: true, Cols: []digest.Col{{Name: "c", TypeStr: "int"}}})),
		"statement kind": feats(t, featCase, "rename t>u", nil, nil,
			&digest.Digest{Verdict: "accept", Stmts: []digest.Stmt{{Kind: "rename_table", Pairs: []digest.Pair{{OldTable: "t", NewTable: "u"}}}}}),
		"verdict": feats(t, featCase, "", errors.New("boom"), nil,
			&digest.Digest{Verdict: "reject", Error: "boom"}),
	}
	engineB := featCase
	engineB.Engine = run.EngineMariaDB
	distinct["engine"] = feats(t, engineB, "alter t{col c=int32}", nil, nil, base)
	modeB := featCase
	modeB.SQLMode = SQLModeOracle
	distinct["sql mode"] = feats(t, modeB, "alter t{col c=int32}", nil, nil, base)
	for name, f := range distinct {
		if f == baseFeats {
			t.Errorf("%s variation collapsed into base features %s", name, f)
		}
	}
}

func TestBehaviorFeaturesModeMasked(t *testing.T) {
	unmasked := featCase
	unmasked.SQLMode = featCase.SQLMode | 1<<63
	d := acceptAlter(digest.Spec{Op: "add", Cols: []digest.Col{{Name: "c", TypeStr: "int"}}})
	if feats(t, featCase, "alter t{col c=int32}", nil, nil, d) != feats(t, unmasked, "alter t{col c=int32}", nil, nil, d) {
		t.Fatal("non-descriptor sql_mode bit changed features")
	}
}

func TestBehaviorFeaturesRejectErrorClasses(t *testing.T) {
	rejA := &digest.Digest{Verdict: "reject", Error: `You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'foo bar' at line 1`}
	rejB := &digest.Digest{Verdict: "reject", Error: `You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'entirely(different' at line 42`}
	errA := errors.New(`parse error at byte 21: unexpected token "abc"`)
	errB := errors.New(`parse error at byte 7: unexpected token "xyz"`)
	if feats(t, featCase, "", errA, nil, rejA) != feats(t, featCase, "", errB, nil, rejB) {
		t.Fatal("identifier/position-only error variation changed features")
	}
	rejC := &digest.Digest{Verdict: "reject", Error: `Duplicate column name 'c'`}
	if feats(t, featCase, "", errA, nil, rejA) == feats(t, featCase, "", errA, nil, rejC) {
		t.Fatal("distinct oracle error classes collapsed")
	}
}

func TestBehaviorFeaturesPanicAndTimeout(t *testing.T) {
	d := acceptAlter(digest.Spec{Op: "add", Cols: []digest.Col{{Name: "c", TypeStr: "int"}}})
	plain := feats(t, featCase, "", nil, nil, d)
	pan := feats(t, featCase, "", nil, &ddllexec.PanicInfo{Value: "boom"}, d)
	timeout := feats(t, featCase, "", nil, &ddllexec.PanicInfo{Value: "boom", Timeout: true}, d)
	if plain == pan || plain == timeout || pan == timeout {
		t.Fatalf("panic/timeout classes collapsed:\nplain=%s\npan=%s\ntimeout=%s", plain, pan, timeout)
	}
}

func TestBehaviorFeaturesSkipAndStatementCap(t *testing.T) {
	skip := &digest.Digest{Verdict: "accept", Stmts: []digest.Stmt{{Kind: "other"}, {Kind: "alter_table", Table: "t"}}}
	lone := &digest.Digest{Verdict: "accept", Stmts: []digest.Stmt{{Kind: "alter_table", Table: "t"}}}
	if feats(t, featCase, "", nil, nil, skip) == feats(t, featCase, "", nil, nil, lone) {
		t.Fatal("oracle skip class collapsed with plain accept")
	}
	alters := func(n int) *digest.Digest {
		d := &digest.Digest{Verdict: "accept"}
		for range n {
			d.Stmts = append(d.Stmts, digest.Stmt{Kind: "alter_table", Table: "t"})
		}
		return d
	}
	if feats(t, featCase, "", nil, nil, alters(5)) != feats(t, featCase, "", nil, nil, alters(7)) {
		t.Fatal("statement lists beyond the cap should share features")
	}
	if feats(t, featCase, "", nil, nil, alters(1)) == feats(t, featCase, "", nil, nil, alters(2)) {
		t.Fatal("1 vs 2 statements collapsed")
	}
}

func TestBehaviorFeaturesFamilyIsSeparateFeature(t *testing.T) {
	intD := acceptAlter(digest.Spec{Op: "add", Cols: []digest.Col{{Name: "c", TypeStr: "int"}}})
	textD := acceptAlter(digest.Spec{Op: "add", Cols: []digest.Col{{Name: "c", TypeStr: "text"}}})
	// int/int32 spell the same family on both sides (digits drop); text/string
	// are one family per side.
	intF := BehaviorFeatures(featCase, "alter t{col c=int32}", nil, nil, intD, nil)
	textF := BehaviorFeatures(featCase, "alter t{col c=string}", nil, nil, textD, nil)
	if len(intF) != 2 || len(textF) != 3 {
		t.Fatalf("want structural+family features (2 and 3), got %d and %d", len(intF), len(textF))
	}
	if intF[0] != textF[0] {
		t.Fatalf("structural feature should ignore type family: %#x != %#x", intF[0], textF[0])
	}
	if intF[1] == textF[1] || intF[1] == textF[2] {
		t.Fatal("family features collapsed across families")
	}
	if n := len(BehaviorFeatures(featCase, "alter t{col c=int32, d=int32}", nil, nil,
		acceptAlter(digest.Spec{Op: "add", Cols: []digest.Col{{Name: "c", TypeStr: "int"}, {Name: "d", TypeStr: "int(11)"}}}), nil)); n != 2 {
		t.Fatalf("duplicate families not deduped: %d features", n)
	}
}
