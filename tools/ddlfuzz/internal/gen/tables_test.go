package gen

import (
	"math/rand/v2"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"
)

func TestChecklistTablesMatch(t *testing.T) {
	want := map[string]bool{}
	for _, id := range Checklist {
		if want[id] {
			t.Fatalf("duplicate checklist id %q", id)
		}
		want[id] = false
	}
	for _, e := range allEntries() {
		if _, ok := want[e.ID]; !ok {
			t.Fatalf("entry id %q missing from checklist", e.ID)
		}
		want[e.ID] = true
	}
	for id, seen := range want {
		if !seen {
			t.Fatalf("checklist id %q has no entry", id)
		}
	}
}

func BenchmarkGenerate(b *testing.B) {
	r := rand.New(rand.NewPCG(7, 8))
	for i := 0; i < b.N; i++ {
		mode := ChooseMode(r, true)
		_ = Generate(r, true, mode)
	}
}

func TestGenerateFuzzUTF8AndSentinel(t *testing.T) {
	for _, isMaria := range []bool{false, true} {
		r := rand.New(rand.NewPCG(11, uint64(map[bool]uint64{false: 22, true: 33}[isMaria])))
		for i := 0; i < 50000; i++ {
			mode := ChooseMode(r, isMaria)
			sql := Generate(r, isMaria, mode)
			if !utf8.ValidString(sql) {
				t.Fatalf("invalid utf8 maria=%v mode=%d sql=%q", isMaria, mode, sql)
			}
			if strings.Contains(sql, sentinelDB) {
				t.Fatalf("sentinel emitted: %q", sql)
			}
		}
	}
}

func TestReservedWordLists(t *testing.T) {
	tests := []struct {
		name string
		set  map[string]struct{}
		min  int
	}{
		{"mysql", reservedMySQL(), 200},
		{"mariadb", reservedMariaDB(), 250},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.set) <= tt.min {
				t.Fatalf("reserved count = %d, want > %d", len(tt.set), tt.min)
			}
			for word := range tt.set {
				for i := 0; i < len(word); i++ {
					if word[i] > unicode.MaxASCII {
						t.Fatalf("non-ascii reserved word %q", word)
					}
				}
			}
			for _, word := range []string{"select", "table", "column", "key"} {
				if _, ok := tt.set[word]; !ok {
					t.Fatalf("reserved word %q missing", word)
				}
			}
		})
	}
}

func TestQuoteMaybeReservedWordsMostlyQuoted(t *testing.T) {
	for _, isMaria := range []bool{false, true} {
		c := &Ctx{R: rand.New(rand.NewPCG(101, 202)), IsMariaDB: isMaria}
		quoted := 0
		for i := 0; i < 10000; i++ {
			if quoteMaybe(c, "select") != "select" {
				quoted++
			}
		}
		if quoted < 8500 {
			t.Fatalf("isMaria=%v quoted %d/10000, want >= 8500", isMaria, quoted)
		}
	}
}
