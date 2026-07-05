package main

import (
	"bytes"
	"database/sql"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/corpus"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/digest"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/run"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/sancov"
)

// storedFeature reads the feature column straight off the corpus file (the
// sqlite driver is registered by the corpus import).
func storedFeature(t *testing.T, store *corpus.Store, c run.Case) string {
	t.Helper()
	db, err := sql.Open("sqlite", store.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var feature sql.NullString
	if err := db.QueryRow(
		`SELECT feature FROM corpus WHERE engine = ? AND hash = ?`,
		run.EngineName(c.Engine), corpus.Hash(c.SQL, c.SQLMode),
	).Scan(&feature); err != nil {
		t.Fatal(err)
	}
	return feature.String
}

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
			"mysql":   {baseKeys: map[string]struct{}{}, recentKeys: map[string]int{}},
			"mariadb": {baseKeys: map[string]struct{}{}, recentKeys: map[string]int{}},
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

func TestRetainBehaviorVirginBitmap(t *testing.T) {
	store, err := corpus.Open(filepath.Join(t.TempDir(), "corpus.db"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	loop := &fuzzLoop{
		store: store,
		engines: map[string]*engineState{
			"mysql":   {baseKeys: map[string]struct{}{}, recentKeys: map[string]int{}},
			"mariadb": {baseKeys: map[string]struct{}{}, recentKeys: map[string]int{}},
		},
	}
	accept := func(name, typeStr string) *digest.Digest {
		return &digest.Digest{Verdict: "accept", Stmts: []digest.Stmt{{Kind: "alter_table", Table: "t", Specs: []digest.Spec{{Op: "add", Cols: []digest.Col{{Name: name, TypeStr: typeStr}}}}}}}
	}
	c := run.Case{SQL: []byte("ALTER TABLE t ADD c INT"), Engine: run.EngineMySQL}
	feats := loop.retainBehavior(nil, c, "alter t{col c=int32}", nil, nil, accept("c", "int"))
	es := loop.engines["mysql"]
	if es.behaviorBitsSet != 2 || store.Count("mysql") != 1 {
		t.Fatalf("first class: bits=%d corpus=%d, want 2/1", es.behaviorBitsSet, store.Count("mysql"))
	}
	if got := storedFeature(t, store, c); got != strconv.FormatUint(feats[0], 10) {
		t.Fatalf("retained row feature = %q, want structural feature %d", got, feats[0])
	}
	c2 := run.Case{SQL: []byte("ALTER TABLE u ADD d INT"), Engine: run.EngineMySQL}
	loop.retainBehavior(nil, c2, "alter u{col d=int32}", nil, nil, accept("d", "int"))
	if es.behaviorBitsSet != 2 || store.Count("mysql") != 1 {
		t.Fatalf("identifier variation: bits=%d corpus=%d, want 2/1", es.behaviorBitsSet, store.Count("mysql"))
	}
	c3 := run.Case{SQL: []byte("ALTER TABLE t ADD c TEXT"), Engine: run.EngineMySQL}
	loop.retainBehavior(nil, c3, "alter t{col c=string}", nil, nil, accept("c", "text"))
	if es.behaviorBitsSet != 4 || store.Count("mysql") != 2 {
		t.Fatalf("family variation: bits=%d corpus=%d, want 4/2", es.behaviorBitsSet, store.Count("mysql"))
	}
	c4 := run.Case{SQL: []byte("ALTER TABLE t DROP c"), Engine: run.EngineMySQL}
	d4 := &digest.Digest{Verdict: "accept", Stmts: []digest.Stmt{{Kind: "alter_table", Table: "t", Specs: []digest.Spec{{Op: "drop", OldName: "c"}}}}}
	loop.retainBehavior(nil, c4, "alter t{drop c}", nil, nil, d4)
	if es.behaviorBitsSet != 5 || store.Count("mysql") != 3 {
		t.Fatalf("structural variation: bits=%d corpus=%d, want 5/3", es.behaviorBitsSet, store.Count("mysql"))
	}
	if loop.retainedTotal != 3 {
		t.Fatalf("retainedTotal=%d, want 3", loop.retainedTotal)
	}
}

func TestBehaviorBitsPersistAcrossRestart(t *testing.T) {
	stateDir := t.TempDir()
	newLoop := func(dbName string) *fuzzLoop {
		store, err := corpus.Open(filepath.Join(stateDir, dbName), 0)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { store.Close() })
		loop := &fuzzLoop{
			cfg:     config{stateDir: stateDir},
			store:   store,
			engines: map[string]*engineState{},
		}
		for _, engine := range []string{"mysql", "mariadb"} {
			acc, err := sancov.Load(filepath.Join(stateDir, "coverage", engine+".sancov"), engine)
			if err != nil {
				t.Fatal(err)
			}
			es := &engineState{accum: acc, baseKeys: map[string]struct{}{}, recentKeys: map[string]int{}}
			if err := es.loadBehaviorBits(behaviorBitsPath(stateDir, engine)); err != nil {
				t.Fatal(err)
			}
			loop.engines[engine] = es
		}
		return loop
	}
	acceptInt := &digest.Digest{Verdict: "accept", Stmts: []digest.Stmt{{Kind: "alter_table", Table: "t", Specs: []digest.Spec{{Op: "add", Cols: []digest.Col{{Name: "c", TypeStr: "int"}}}}}}}
	c := run.Case{SQL: []byte("ALTER TABLE t ADD c INT"), Engine: run.EngineMySQL}

	loop1 := newLoop("corpus1.db")
	if got := loop1.engines["mysql"].behaviorBitsSet; got != 0 {
		t.Fatalf("missing file should load fresh, bits=%d", got)
	}
	loop1.retainBehavior(nil, c, "alter t{col c=int32}", nil, nil, acceptInt)
	setBefore := loop1.engines["mysql"].behaviorBitsSet
	if setBefore == 0 || loop1.retainedTotal != 1 {
		t.Fatalf("first run: bits=%d retained=%d", setBefore, loop1.retainedTotal)
	}
	loop1.saveCoverage()

	tmps, err := filepath.Glob(filepath.Join(stateDir, "coverage", ".*.tmp-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(tmps) != 0 {
		t.Fatalf("save left tmp files behind: %v", tmps)
	}

	loop2 := newLoop("corpus2.db")
	es := loop2.engines["mysql"]
	if es.behaviorBitsSet != setBefore {
		t.Fatalf("restart: bits=%d, want %d", es.behaviorBitsSet, setBefore)
	}
	if bits := es.behaviorBits; bits != loop1.engines["mysql"].behaviorBits {
		t.Fatal("restart: bitmap contents differ")
	}
	loop2.retainBehavior(nil, c, "alter t{col c=int32}", nil, nil, acceptInt)
	if es.behaviorBitsSet != setBefore || loop2.retainedTotal != 0 {
		t.Fatalf("restart replayed retention: bits=%d retained=%d", es.behaviorBitsSet, loop2.retainedTotal)
	}
	d := &digest.Digest{Verdict: "accept", Stmts: []digest.Stmt{{Kind: "alter_table", Table: "t", Specs: []digest.Spec{{Op: "drop", OldName: "c"}}}}}
	loop2.retainBehavior(nil, run.Case{SQL: []byte("ALTER TABLE t DROP c"), Engine: run.EngineMySQL}, "alter t{drop c}", nil, nil, d)
	if es.behaviorBitsSet <= setBefore || loop2.retainedTotal != 1 {
		t.Fatalf("fresh class after restart: bits=%d retained=%d", es.behaviorBitsSet, loop2.retainedTotal)
	}

	// A stale file (map size changed) loads fresh rather than misaligned.
	if err := os.WriteFile(behaviorBitsPath(stateDir, "mysql"), []byte("short"), 0o644); err != nil {
		t.Fatal(err)
	}
	loop3 := newLoop("corpus3.db")
	if got := loop3.engines["mysql"].behaviorBitsSet; got != 0 {
		t.Fatalf("stale-size file should load fresh, bits=%d", got)
	}
}
