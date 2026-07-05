package gen

import (
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
)

func TestFreshPoolPalettePreferredAndLiveFiltered(t *testing.T) {
	palette := []string{"a", "b", "c", "d"}
	live := []string{"b", "x"}
	p := NewFreshPool(palette, live)
	r := rand.New(rand.NewPCG(1, 2))
	seen := map[string]bool{}
	for range 3 {
		name := p.Next(r)
		if !slices.Contains(palette, name) {
			t.Fatalf("synthesized %q before palette exhausted", name)
		}
		if name == "b" {
			t.Fatal("returned live palette name")
		}
		if seen[name] {
			t.Fatalf("repeated name %q", name)
		}
		seen[name] = true
	}
	if name := p.Next(r); slices.Contains(palette, name) {
		t.Fatalf("exhausted pool returned palette name %q", name)
	}
}

func TestFreshPoolDeterministicFirstFree(t *testing.T) {
	p := NewFreshPool([]string{"a", "b", "c"}, []string{"a"})
	if got := p.Next(nil); got != "b" {
		t.Fatalf("first free = %q, want b", got)
	}
	if got := p.Next(nil); got != "c" {
		t.Fatalf("second free = %q, want c", got)
	}
	if got := p.Next(nil); got != "nf1_1" {
		t.Fatalf("deterministic synthesis = %q, want nf1_1", got)
	}
	if got := p.Next(nil); got != "nf2_2" {
		t.Fatalf("deterministic synthesis = %q, want nf2_2", got)
	}
}

func TestFreshPoolSynthesisDistinctAndAvoidsLive(t *testing.T) {
	live := []string{"nf1_1", "nf2_2"}
	p := NewFreshPool(nil, live)
	seen := map[string]bool{}
	for range 20 {
		name := p.Next(nil)
		if slices.Contains(live, name) {
			t.Fatalf("synthesis returned live name %q", name)
		}
		if seen[name] {
			t.Fatalf("synthesis repeated %q", name)
		}
		if !strings.HasPrefix(name, "nf") {
			t.Fatalf("unexpected synthesis spelling %q", name)
		}
		seen[name] = true
	}

	r := rand.New(rand.NewPCG(3, 4))
	p = NewFreshPool(nil, nil)
	seen = map[string]bool{}
	for range 100 {
		name := p.Next(r)
		if seen[name] {
			t.Fatalf("random synthesis repeated %q", name)
		}
		seen[name] = true
	}
}

func TestFreshPoolDuplicatePaletteEntries(t *testing.T) {
	p := NewFreshPool([]string{"a", "a", "b"}, nil)
	if got := p.Next(nil); got != "a" {
		t.Fatalf("first = %q, want a", got)
	}
	if got := p.Next(nil); got != "b" {
		t.Fatalf("second = %q, want b (duplicate skipped)", got)
	}
}
