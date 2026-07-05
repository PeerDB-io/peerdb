package compare

import "testing"

// Inputs are verbatim server error text (MySQL 9.x / MariaDB 11+ message
// formats) plus our parser's error spellings; the table pins the mask
// behavior for each leak class: single-quoted spans, backticked spans,
// "at line N", bare digit runs, and near-fragments whose quoted span is
// itself verbatim input (may contain quotes). Two maskers implement these
// rules independently — NormalizeError (offline) and featureHasher's
// maskedError (bitmap hot path) — so every case runs through both.
var realServerErrorTable = []struct {
	name string
	in   string
	want string
}{
	{
		name: "mysql syntax error",
		in:   `You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'ADD COLUMN c' at line 1`,
		want: `You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '?' at line ?`,
	},
	{
		name: "mariadb syntax error",
		in:   `You have an error in your SQL syntax; check the manual that corresponds to your MariaDB server version for the right syntax to use near 'WAIT 3' at line 2`,
		want: `You have an error in your SQL syntax; check the manual that corresponds to your MariaDB server version for the right syntax to use near '?' at line ?`,
	},
	{
		name: "near fragment starting with a quoted literal",
		in:   `You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near ''x' INT' at line 1`,
		want: `You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '?' at line ?`,
	},
	{
		name: "unknown column",
		in:   `Unknown column 'c1' in 't1'`,
		want: `Unknown column '?' in '?'`,
	},
	{
		name: "duplicate column",
		in:   `Duplicate column name 'c2'`,
		want: `Duplicate column name '?'`,
	},
	{
		name: "apostrophe in word does not flip quote parity",
		in:   `Table 'db1.t1' doesn't exist`,
		want: `Table '?' doesn't exist`,
	},
	{
		name: "trailing quoted span after apostrophe word",
		in:   `Key column 'c3' doesn't exist in table 'db1.t9'`,
		want: `Key column '?' doesn't exist in table '?'`,
	},
	{
		name: "mariadb backticked identifier",
		in:   "Can't DROP COLUMN `c1`; check that it exists",
		want: "Can't DROP COLUMN `?`; check that it exists",
	},
	{
		name: "bare digits",
		in:   `Too big precision 100 specified for 'c'. Maximum is 65.`,
		want: `Too big precision ? specified for '?'. Maximum is ?.`,
	},
	{
		name: "row size limit",
		in:   `Row size too large. The maximum row size for the used table type, not counting BLOBs, is 65535. This includes storage overhead, check the manual. You have to change some columns to TEXT or BLOBs`,
		want: `Row size too large. The maximum row size for the used table type, not counting BLOBs, is ?. This includes storage overhead, check the manual. You have to change some columns to TEXT or BLOBs`,
	},
	{
		name: "our parser byte offset and %q fragment",
		in:   `parse error at byte 21: unexpected token "foo"`,
		want: `parse error at byte ?: unexpected token "?"`,
	},
	{
		name: "whitespace collapse",
		in:   "syntax\terror\n  near 'x\ny'",
		want: `syntax error near '?'`,
	},
	{
		name: "identifier with digits inside quotes",
		in:   `Unknown column 'nf3_7' in 't1'`,
		want: `Unknown column '?' in '?'`,
	},
}

// Same error class, differing only in input-derived content (identifiers,
// fragments, positions); both maskers must collapse each pair.
var errorCollapsePairs = [][2]string{
	{
		`You have an error in your SQL syntax; check the manual that corresponds to your MariaDB server version for the right syntax to use near 'x1' at line 1`,
		`You have an error in your SQL syntax; check the manual that corresponds to your MariaDB server version for the right syntax to use near 'completely different (fragment)' at line 12`,
	},
	{
		`You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near ''x' INT' at line 1`,
		`You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near ''yz' VARCHAR(3)' at line 22`,
	},
	{"Can't DROP COLUMN `a`; check that it exists", "Can't DROP COLUMN `b2`; check that it exists"},
	{`Table 'db1.t1' doesn't exist`, `Table 'other.name' doesn't exist`},
	{`Key column 'c3' doesn't exist in table 'db1.t9'`, `Key column 'nf1_4' doesn't exist in table 'other.tbl'`},
	{`parse error at byte 3: unexpected token "x"`, `parse error at byte 999: unexpected token "yy"`},
	{"syntax\terror\n  near 'x\ny'", "syntax error near 'ab'"},
}

func TestNormalizeErrorRealServerText(t *testing.T) {
	for _, tc := range realServerErrorTable {
		t.Run(tc.name, func(t *testing.T) {
			if got := NormalizeError(tc.in); got != tc.want {
				t.Fatalf("NormalizeError(%q)\n got %q\nwant %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestNormalizeErrorCollapsesIdentifierVariants(t *testing.T) {
	for i, p := range errorCollapsePairs {
		if a, b := NormalizeError(p[0]), NormalizeError(p[1]); a != b {
			t.Errorf("pair %d did not collapse:\n%q\n%q", i, a, b)
		}
	}
}

func maskedErrorHash(s string) uint64 {
	f := featureHasher{sum: fnvOffset64}
	f.maskedError(s)
	return f.sum
}

// The hot-path masker must induce the same equivalence classes over the real
// server corpus as NormalizeError: collapse the pairs, and agree with the
// table's want strings on which inputs share a class and which stay distinct.
func TestMaskedErrorRealServerText(t *testing.T) {
	for i, p := range errorCollapsePairs {
		if maskedErrorHash(p[0]) != maskedErrorHash(p[1]) {
			t.Errorf("pair %d did not collapse on the hot path:\n%q\n%q", i, p[0], p[1])
		}
	}
	for i, a := range realServerErrorTable {
		for _, b := range realServerErrorTable[i+1:] {
			sameClass := a.want == b.want
			collapsed := maskedErrorHash(a.in) == maskedErrorHash(b.in)
			if collapsed != sameClass {
				t.Errorf("maskedError(%s) vs (%s): collapsed=%v, want %v", a.name, b.name, collapsed, sameClass)
			}
		}
	}
}

func TestNormalizePanicMasksAddressesAndDigits(t *testing.T) {
	a := NormalizePanic("runtime error: index out of range [5] with length 3", nil)
	b := NormalizePanic("runtime error: index out of range [17] with length 9", nil)
	if a != b {
		t.Fatalf("index-out-of-range panics did not collapse: %q vs %q", a, b)
	}
	c := NormalizePanic("invalid memory address 0x140012ab000", nil)
	d := NormalizePanic("invalid memory address 0xc000f00d20", nil)
	if c != d {
		t.Fatalf("address panics did not collapse: %q vs %q", c, d)
	}
}
