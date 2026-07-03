package digest

import (
	"encoding/json"
	"sync"
)

type Digest struct {
	Verdict string `json:"verdict"` // "accept" | "reject"
	Error   string `json:"error,omitempty"`
	Stmts   []Stmt `json:"stmts,omitempty"`
}
type Stmt struct {
	Kind      string `json:"kind"` // other | alter_table | rename_table
	Schema    string `json:"schema"`
	Table     string `json:"table"`
	NewSchema string `json:"new_schema,omitempty"`
	NewTable  string `json:"new_table,omitempty"`
	Specs     []Spec `json:"specs"`
	Pairs     []Pair `json:"pairs"`
}
type Spec struct {
	Op          string `json:"op"` // add | modify | change | drop | rename_col
	OldName     string `json:"old_name"`
	NewName     string `json:"new_name"`
	Cols        []Col  `json:"cols"`
	HasPosition bool   `json:"has_position"`
}
type Pair struct {
	OldSchema string `json:"old_schema"`
	OldTable  string `json:"old_table"`
	NewSchema string `json:"new_schema"`
	NewTable  string `json:"new_table"`
}
type Col struct {
	Name          string `json:"name"`
	TypeStr       string `json:"type_str"`
	NotNull       bool   `json:"not_null"`
	ParamsWritten []int  `json:"params_written"` // nil when JSON null
}

var pool = sync.Pool{New: func() any { return new(Digest) }}

func Acquire() *Digest {
	d := pool.Get().(*Digest)
	d.Reset()
	return d
}

func Release(d *Digest) {
	if d == nil {
		return
	}
	d.Reset()
	pool.Put(d)
}

func Decode(b []byte) (*Digest, error) {
	d := Acquire()
	if err := json.Unmarshal(b, d); err != nil {
		Release(d)
		return nil, err
	}
	return d, nil
}

func (d *Digest) Reset() {
	d.Verdict = ""
	d.Error = ""
	for i := range d.Stmts {
		d.Stmts[i].Specs = nil
		d.Stmts[i].Pairs = nil
		d.Stmts[i].NewSchema = ""
		d.Stmts[i].NewTable = ""
	}
	d.Stmts = d.Stmts[:0]
}
