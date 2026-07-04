package fuzzcmd

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/corpus"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/run"
)

func TestLoadCorpusBasesTierBudget(t *testing.T) {
	store, err := corpus.Open(filepath.Join(t.TempDir(), "corpus.db"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	add := func(signal uint8, n, size int) {
		for i := range n {
			sql := append([]byte("ALTER TABLE t ADD c"), bytes.Repeat([]byte{'x'}, size)...)
			sql = append(sql, byte(i), byte(signal))
			c := run.Case{SQL: sql, Engine: run.EngineMySQL, Signal: signal}
			if ok, err := store.AddSignal(c, signal); err != nil || !ok {
				t.Fatalf("add signal=%d i=%d ok=%v err=%v", signal, i, ok, err)
			}
		}
	}
	add(run.SignalBehavior, 3, 10)
	add(run.SignalOracleEdge, 2, 10)
	add(run.SignalNoise, 20, 1<<20)

	loop := &fuzzLoop{
		store:     store,
		seedValue: 1,
		engines: map[string]*engineState{
			"mysql":   {baseKeys: map[string]struct{}{}, recentKeys: map[string]int{}, behaviorSeen: map[string]struct{}{}},
			"mariadb": {baseKeys: map[string]struct{}{}, recentKeys: map[string]int{}, behaviorSeen: map[string]struct{}{}},
		},
	}
	if err := loop.loadCorpusBases(); err != nil {
		t.Fatal(err)
	}
	es := loop.engines["mysql"]
	es.mu.Lock()
	defer es.mu.Unlock()
	var interesting, noise int
	var noiseBytes int64
	for _, c := range es.bases {
		switch c.Signal {
		case run.SignalBehavior, run.SignalOracleEdge:
			interesting++
		default:
			noise++
			noiseBytes += int64(len(c.SQL))
		}
	}
	if interesting != 5 {
		t.Fatalf("interesting bases=%d, want 5", interesting)
	}
	if noise == 0 {
		t.Fatal("noise reservoir empty")
	}
	if noiseBytes > basePoolByteCap/20+(1<<20) {
		t.Fatalf("noise bytes=%d over cap", noiseBytes)
	}
}
