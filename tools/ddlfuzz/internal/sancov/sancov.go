package sancov

import (
	"os"
	"path/filepath"
	"unsafe"
)

// Accumulator OR-accumulates an oracle's cumulative inline-8bit coverage
// counters. Only zero/nonzero per byte matters (an edge is "covered" once its
// counter is nonzero), so edge count is popcount-of-nonzero-bytes and Merge
// need not preserve exact counter values — it only has to make covered bytes
// nonzero. edges is maintained incrementally so EdgeCount is O(1).
type Accumulator struct {
	Engine string
	Bits   []byte
	edges  int
}

func Load(path, engine string) (*Accumulator, error) {
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return &Accumulator{Engine: engine}, nil
	}
	if err != nil {
		return nil, err
	}
	a := &Accumulator{Engine: engine, Bits: b}
	a.edges = countNonzero(b)
	return a, nil
}

// Merge folds a fresh coverage snapshot into the accumulator. The oracle's
// bitmap spans the whole server binary but only the small parser region is
// ever hit, so the map is overwhelmingly zero; processing 8 bytes at a time
// and skipping words that add no new coverage turns a full 244 MB byte scan
// into a near-free pass once coverage plateaus.
func (a *Accumulator) Merge(counters []byte) (grew bool, newEdges int) {
	n := len(counters)
	if n == 0 {
		return false, 0
	}
	if len(a.Bits) < n {
		a.Bits = append(a.Bits, make([]byte, n-len(a.Bits))...)
	}
	nWords := n / 8
	if nWords > 0 {
		accWords := unsafe.Slice((*uint64)(unsafe.Pointer(&a.Bits[0])), nWords)
		newWords := unsafe.Slice((*uint64)(unsafe.Pointer(&counters[0])), nWords)
		for w := range nWords {
			nv := newWords[w]
			if nv == 0 {
				continue
			}
			av := accWords[w]
			if nv&^av == 0 {
				// no byte transitions from zero to nonzero in this word;
				// the covered-edge set is unchanged, so skip the write
				continue
			}
			base := w * 8
			for k := range 8 {
				if counters[base+k] != 0 && a.Bits[base+k] == 0 {
					grew = true
					newEdges++
				}
			}
			accWords[w] = av | nv
		}
	}
	for i := nWords * 8; i < n; i++ {
		v := counters[i]
		if v != 0 && a.Bits[i] == 0 {
			grew = true
			newEdges++
		}
		a.Bits[i] |= v
	}
	a.edges += newEdges
	return grew, newEdges
}

func (a *Accumulator) EdgeCount() int {
	return a.edges
}

func countNonzero(b []byte) int {
	n := 0
	for _, v := range b {
		if v != 0 {
			n++
		}
	}
	return n
}

func (a *Accumulator) Save(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	if _, err := tmp.Write(a.Bits); err != nil {
		tmp.Close()
		_ = os.Remove(name)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(name)
		return err
	}
	return os.Rename(name, path)
}
