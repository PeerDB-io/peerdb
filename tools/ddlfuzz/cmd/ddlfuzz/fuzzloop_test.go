package main

import (
	"bytes"
	"database/sql"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/compare"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/corpus"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/digest"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/run"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/sancov"
)

func TestMutationBaseSnapshotPopulation(t *testing.T) {
	loop := &fuzzLoop{
		engines: map[string]*engineState{
			"mysql":   {baseKeys: map[string]struct{}{}, recentKeys: map[string]int{}},
			"mariadb": {baseKeys: map[string]struct{}{}, recentKeys: map[string]int{}},
		},
	}
	mysql := loop.engines["mysql"]
	for i := range 3 {
		mysql.pushRecentLocked(run.Case{SQL: []byte("recent" + strconv.Itoa(i)), Engine: run.EngineMySQL, Signal: run.SignalBehavior})
	}
	for i := range 4 {
		mysql.bases = append(mysql.bases, run.Case{SQL: []byte("signal" + strconv.Itoa(i)), Engine: run.EngineMySQL, Signal: run.SignalBehavior})
	}
	for i := range 2 {
		mysql.bases = append(mysql.bases, run.Case{SQL: []byte("noise" + strconv.Itoa(i)), Engine: run.EngineMySQL, Signal: run.SignalNoise})
	}
	maria := loop.engines["mariadb"]
	for i := range 2 {
		maria.bases = append(maria.bases, run.Case{SQL: []byte("opposite" + strconv.Itoa(i)), Engine: run.EngineMariaDB, Signal: run.SignalOracleEdge})
	}
	maria.bases = append(maria.bases, run.Case{SQL: []byte("opposite-noise"), Engine: run.EngineMariaDB, Signal: run.SignalNoise})

	var snap mutationBaseSnapshot
	loop.fillSnapshot(rand.New(rand.NewPCG(9, 10)), run.EngineMySQL, &snap)
	if len(snap.recent) != 3 || len(snap.oldSignal) != 4 || len(snap.oldNoise) != 2 || len(snap.oppositeSignal) != 2 {
		t.Fatalf("snapshot lens recent/signal/noise/opposite=%d/%d/%d/%d, want 3/4/2/2",
			len(snap.recent), len(snap.oldSignal), len(snap.oldNoise), len(snap.oppositeSignal))
	}
	for _, c := range snap.oldSignal {
		if c.Signal == run.SignalNoise {
			t.Fatalf("oldSignal contains noise case %q", c.SQL)
		}
	}
	for _, c := range snap.oldNoise {
		if c.Signal != run.SignalNoise {
			t.Fatalf("oldNoise contains signal case %q", c.SQL)
		}
	}
	for _, c := range snap.oppositeSignal {
		if c.Engine != run.EngineMariaDB || c.Signal == run.SignalNoise {
			t.Fatalf("oppositeSignal contains wrong case engine=%d signal=%d", c.Engine, c.Signal)
		}
	}

	noiseOnly := &mutationBaseSnapshot{oldNoise: []run.Case{{SQL: []byte("noise-only"), Signal: run.SignalNoise}}}
	if c, ok := pickOldBaseFromSnapshot(rand.New(rand.NewPCG(1, 2)), noiseOnly); !ok || c.Signal != run.SignalNoise {
		t.Fatalf("old pick did not fall back to noise-only pool: ok=%v signal=%d", ok, c.Signal)
	}
	recentOnly := &mutationBaseSnapshot{recent: []run.Case{{SQL: []byte("recent-only"), Signal: run.SignalBehavior}}}
	if c, tier, ok := pickBaseFromSnapshot(rand.New(rand.NewPCG(3, 4)), recentOnly); !ok || tier != run.BaseTierRecent || !bytes.Equal(c.SQL, []byte("recent-only")) {
		t.Fatalf("base pick did not fall back to recent-only pool: ok=%v tier=%d sql=%q", ok, tier, c.SQL)
	}
}

func TestNextCaseFromSnapshotDoesNotLockEngines(t *testing.T) {
	loop := &fuzzLoop{
		cfg: config{mutRatio: 1},
		engines: map[string]*engineState{
			"mysql":   {baseKeys: map[string]struct{}{}, recentKeys: map[string]int{}},
			"mariadb": {baseKeys: map[string]struct{}{}, recentKeys: map[string]int{}},
		},
	}
	snap := &mutationBaseSnapshot{
		oldSignal:      []run.Case{{SQL: []byte("ALTER TABLE t ADD c INT"), Engine: run.EngineMySQL, Signal: run.SignalBehavior}},
		oppositeSignal: []run.Case{{SQL: []byte("ALTER TABLE u ADD d INT"), Engine: run.EngineMariaDB, Signal: run.SignalBehavior}},
	}
	loop.engines["mysql"].mu.Lock()
	loop.engines["mariadb"].mu.Lock()
	defer loop.engines["mariadb"].mu.Unlock()
	defer loop.engines["mysql"].mu.Unlock()

	done := make(chan run.Case, 1)
	go func() {
		done <- loop.nextCaseFromSnapshot(rand.New(rand.NewPCG(5, 6)), run.EngineMySQL, snap)
	}()
	select {
	case c := <-done:
		if c.Origin != run.OriginMut || c.Engine != run.EngineMySQL {
			t.Fatalf("nextCaseFromSnapshot returned origin=%d engine=%d, want mut/mysql", c.Origin, c.Engine)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("nextCaseFromSnapshot blocked while engine mutexes were held")
	}
}

func TestPickMutationBaseFromSnapshotDistribution(t *testing.T) {
	loop := &fuzzLoop{cfg: config{mutRatio: 1}}
	snap := &mutationBaseSnapshot{
		recent:    []run.Case{{SQL: []byte("recent"), Signal: run.SignalBehavior}},
		oldSignal: []run.Case{{SQL: []byte("signal-a"), Signal: run.SignalBehavior}, {SQL: []byte("signal-b"), Signal: run.SignalOracleEdge}},
		oldNoise:  []run.Case{{SQL: []byte("noise"), Signal: run.SignalNoise}},
	}
	rng := rand.New(rand.NewPCG(7, 8))
	const draws = 20000
	var recent, old, noise int
	for range draws {
		base, _, tier, ok := loop.pickMutationBaseFromSnapshot(rng, snap)
		if !ok {
			t.Fatal("pickMutationBaseFromSnapshot unexpectedly returned no base")
		}
		switch tier {
		case run.BaseTierRecent:
			recent++
		case run.BaseTierOld:
			old++
			if base.Signal == run.SignalNoise {
				noise++
			}
		default:
			t.Fatalf("unexpected base tier %d", tier)
		}
	}
	if recent < 9000 || recent > 11000 {
		t.Fatalf("recent picks=%d/%d, want roughly half", recent, draws)
	}
	if old < 9000 || old > 11000 {
		t.Fatalf("old picks=%d/%d, want roughly half", old, draws)
	}
	if noise < old*3/100 || noise > old*7/100 {
		t.Fatalf("noise old-picks=%d/%d, want roughly 1/20", noise, old)
	}
}

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
	if es.behaviorBitsSet != 3 || store.Count("mysql") != 1 {
		t.Fatalf("first class: bits=%d corpus=%d, want 3/1", es.behaviorBitsSet, store.Count("mysql"))
	}
	if got := storedFeature(t, store, c); got != strconv.FormatUint(compare.CaseKey(feats), 10) {
		t.Fatalf("retained row feature = %q, want feature fold %d", got, compare.CaseKey(feats))
	}
	c2 := run.Case{SQL: []byte("ALTER TABLE u ADD d INT"), Engine: run.EngineMySQL}
	loop.retainBehavior(nil, c2, "alter u{col d=int32}", nil, nil, accept("d", "int"))
	if es.behaviorBitsSet != 3 || store.Count("mysql") != 1 {
		t.Fatalf("identifier variation: bits=%d corpus=%d, want 3/1", es.behaviorBitsSet, store.Count("mysql"))
	}
	c3 := run.Case{SQL: []byte("ALTER TABLE t ADD c TEXT"), Engine: run.EngineMySQL}
	loop.retainBehavior(nil, c3, "alter t{col c=string}", nil, nil, accept("c", "text"))
	if es.behaviorBitsSet != 5 || store.Count("mysql") != 2 {
		t.Fatalf("family variation: bits=%d corpus=%d, want 5/2", es.behaviorBitsSet, store.Count("mysql"))
	}
	c4 := run.Case{SQL: []byte("ALTER TABLE t DROP c"), Engine: run.EngineMySQL}
	d4 := &digest.Digest{Verdict: "accept", Stmts: []digest.Stmt{{Kind: "alter_table", Table: "t", Specs: []digest.Spec{{Op: "drop", OldName: "c"}}}}}
	loop.retainBehavior(nil, c4, "alter t{drop c}", nil, nil, d4)
	if es.behaviorBitsSet != 6 || store.Count("mysql") != 3 {
		t.Fatalf("structural variation: bits=%d corpus=%d, want 6/3", es.behaviorBitsSet, store.Count("mysql"))
	}
	if loop.retainedTotal != 3 {
		t.Fatalf("retainedTotal=%d, want 3", loop.retainedTotal)
	}
	wantFresh := [4]uint64{1, 2, 0, 3}
	if es.freshBitsByKind != wantFresh {
		t.Fatalf("freshBitsByKind=%v, want %v", es.freshBitsByKind, wantFresh)
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
