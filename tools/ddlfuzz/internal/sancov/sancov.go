package sancov

import (
	"os"
	"path/filepath"
)

type Accumulator struct {
	Engine string
	Bits   []byte
}

func Load(path, engine string) (*Accumulator, error) {
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return &Accumulator{Engine: engine}, nil
	}
	if err != nil {
		return nil, err
	}
	return &Accumulator{Engine: engine, Bits: b}, nil
}

func (a *Accumulator) Merge(counters []byte) (grew bool, newEdges int) {
	if len(a.Bits) < len(counters) {
		a.Bits = append(a.Bits, make([]byte, len(counters)-len(a.Bits))...)
	}
	for i, v := range counters {
		if v != 0 && a.Bits[i] == 0 {
			grew = true
			newEdges++
		}
		a.Bits[i] |= v
	}
	return grew, newEdges
}

func (a *Accumulator) EdgeCount() int {
	n := 0
	for _, v := range a.Bits {
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
