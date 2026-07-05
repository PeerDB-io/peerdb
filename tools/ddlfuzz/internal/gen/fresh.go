package gen

import (
	"fmt"
	"math/rand/v2"
	"slices"
)

// FreshPool allocates names that collide with neither the live set nor any
// name it already handed out: palette names first, synthesis on exhaustion.
type FreshPool struct {
	free []string
	live map[string]bool
	used map[string]bool
	seq  int
}

func NewFreshPool(palette, live []string) *FreshPool {
	liveSet := make(map[string]bool, len(live))
	for _, name := range live {
		liveSet[name] = true
	}
	p := &FreshPool{live: liveSet, used: map[string]bool{}}
	for _, name := range palette {
		if !liveSet[name] {
			p.free = append(p.free, name)
		}
	}
	return p
}

// Next returns a fresh name. r==nil: first-free (deterministic); else uniform
// over free.
func (p *FreshPool) Next(r *rand.Rand) string {
	for len(p.free) > 0 {
		i := 0
		if r != nil {
			i = r.IntN(len(p.free))
		}
		name := p.free[i]
		p.free = slices.Delete(p.free, i, i+1)
		if !p.live[name] && !p.used[name] {
			p.used[name] = true
			return name
		}
	}
	for {
		p.seq++
		n := uint64(p.seq)
		if r != nil {
			n = r.Uint64N(1_000_000)
		}
		name := fmt.Sprintf("nf%d_%d", p.seq, n)
		if !p.live[name] && !p.used[name] {
			p.used[name] = true
			return name
		}
	}
}
