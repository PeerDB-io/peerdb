package compare

import (
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	ddllexec "github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/exec"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/run"
)

// TestKeyspaceProbe is an offline measurement tool, not a correctness test: it
// re-parses saved corpus rows and counts distinct coarse BehaviorFeatures bits
// (expect <= ~10K over the 500K-row sample where the string BehaviorKey was
// near-injective). The oracle digest is not available
// offline, so this measures the our-side slice of the tuple (engine, masked
// mode, flat sig shape, masked error, panic class, per-family bits) with
// d=nil — the digest side runs the same coarsening over the same statement
// space, so its bit count has the same order of magnitude. Guarded behind
// KEYSPACE_PROBE=1 so it never runs in a normal `go test ./...` sweep, and it
// opens the live campaign corpus strictly read-only (file: URI + mode=ro, so
// SQLite enforces it at the C level even though the modernc.org/sqlite driver
// always requests SQLITE_OPEN_READWRITE|CREATE).
func TestKeyspaceProbe(t *testing.T) {
	if os.Getenv("KEYSPACE_PROBE") != "1" {
		t.Skip("set KEYSPACE_PROBE=1 to run this offline measurement")
	}

	dbPath := os.Getenv("CORPUS_DB_PATH")
	if dbPath == "" {
		dbPath = "/Users/ilia/Code/peerdb/tools/ddlfuzz/state/corpus.db"
	}
	behaviorLimit := probeEnvInt(t, "KEYSPACE_PROBE_BEHAVIOR_LIMIT", 300000)
	noiseLimit := probeEnvInt(t, "KEYSPACE_PROBE_NOISE_LIMIT", 200000)
	workers := probeEnvInt(t, "KEYSPACE_PROBE_WORKERS", 8)

	db, err := sql.Open("sqlite", "file:"+dbPath+"?mode=ro")
	if err != nil {
		t.Fatalf("open corpus db: %v", err)
	}
	defer db.Close()

	behaviorRows := loadProbeRows(t, db, "behavior", behaviorLimit)
	noiseRows := loadProbeRows(t, db, "noise", noiseLimit)
	t.Logf("loaded rows: behavior=%d (requested %d), noise=%d (requested %d)",
		len(behaviorRows), behaviorLimit, len(noiseRows), noiseLimit)

	behaviorFeats := computeFeatures(behaviorRows, workers)
	noiseFeats := computeFeatures(noiseRows, workers)

	reportFeatureGroup(t, "behavior", behaviorFeats)
	reportFeatureGroup(t, "noise", noiseFeats)

	combined := map[uint64]int{}
	for _, g := range []map[uint64]int{distinctFeatures(behaviorFeats), distinctFeatures(noiseFeats)} {
		for k, v := range g {
			combined[k] += v
		}
	}
	bits := map[uint64]struct{}{}
	for k := range combined {
		bits[k&((1<<20)-1)] = struct{}{}
	}
	t.Logf("[combined] n=%d distinct coarse classes=%d distinct 2^20 bit indexes=%d (%.2f%% of map)",
		len(behaviorFeats)+len(noiseFeats), len(combined), len(bits), 100*float64(len(bits))/(1<<20))
}

func probeEnvInt(t *testing.T, name string, def int) int {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		t.Fatalf("bad int env %s=%q: %v", name, v, err)
	}
	return n
}

type probeRow struct {
	sql     []byte
	sqlMode uint64
	engine  uint8
}

func loadProbeRows(t *testing.T, db *sql.DB, signal string, limit int) []probeRow {
	t.Helper()
	start := time.Now()
	rows, err := db.Query(
		`SELECT sql, sql_mode, engine FROM corpus WHERE signal = ? ORDER BY RANDOM() LIMIT ?`,
		signal, limit,
	)
	if err != nil {
		t.Fatalf("query signal=%s: %v", signal, err)
	}
	defer rows.Close()

	var out []probeRow
	for rows.Next() {
		var sqlBytes []byte
		var modeText, engineText string
		if err := rows.Scan(&sqlBytes, &modeText, &engineText); err != nil {
			t.Fatalf("scan signal=%s: %v", signal, err)
		}
		mode, err := strconv.ParseUint(modeText, 10, 64)
		if err != nil {
			continue // corrupt sql_mode text; skip rather than fail the whole probe
		}
		engine, err := run.EngineID(engineText)
		if err != nil {
			continue // unrecognized engine text; skip
		}
		out = append(out, probeRow{sql: sqlBytes, sqlMode: mode, engine: engine})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows iteration signal=%s: %v", signal, err)
	}
	t.Logf("loadProbeRows signal=%s: %d rows in %s", signal, len(out), time.Since(start))
	return out
}

type probeFeature struct {
	features []uint64
	outcome  string // accepted | errored | panicked | timeout | empty
}

// computeFeatures re-parses every row with our in-process parser (the same
// worker RunBatch used by the live campaign, see cmd/ddlfuzz/fuzzloop.go
// submitBatch) and reduces each Result to its coarse BehaviorFeatures with
// d=nil (no oracle offline).
func computeFeatures(rows []probeRow, workers int) []probeFeature {
	const batchSize = 256

	type batch struct {
		start int
		cases []run.Case
	}
	batches := make(chan batch)
	results := make([]ddllexec.Result, len(rows))

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			worker := ddllexec.NewWorker(id, 2*time.Second, nil)
			for b := range batches {
				res := worker.RunBatch(b.cases)
				copy(results[b.start:b.start+len(res)], res)
			}
		}(w)
	}

	go func() {
		for start := 0; start < len(rows); start += batchSize {
			end := min(start+batchSize, len(rows))
			cases := make([]run.Case, 0, end-start)
			for _, r := range rows[start:end] {
				cases = append(cases, run.Case{SQL: r.sql, SQLMode: r.sqlMode, Engine: r.engine})
			}
			batches <- batch{start: start, cases: cases}
		}
		close(batches)
	}()
	wg.Wait()

	feats := make([]probeFeature, len(rows))
	for i, r := range rows {
		res := results[i]
		c := run.Case{SQL: r.sql, SQLMode: r.sqlMode, Engine: r.engine}
		f := probeFeature{features: BehaviorFeatures(c, res.Sig, res.Err, res.Panic, nil, nil)}
		switch {
		case res.Panic != nil && res.Panic.Timeout:
			f.outcome = "timeout"
		case res.Panic != nil:
			f.outcome = "panicked"
		case res.Err != nil:
			f.outcome = "errored"
		case res.Sig != "":
			f.outcome = "accepted"
		default:
			f.outcome = "empty"
		}
		feats[i] = f
	}
	return feats
}

func distinctFeatures(feats []probeFeature) map[uint64]int {
	out := map[uint64]int{}
	for _, f := range feats {
		for _, feat := range f.features {
			out[feat]++
		}
	}
	return out
}

func reportFeatureGroup(t *testing.T, group string, feats []probeFeature) {
	t.Helper()
	outcomeCounts := map[string]int{}
	for _, f := range feats {
		outcomeCounts[f.outcome]++
	}
	classes := distinctFeatures(feats)
	sizes := make([]int, 0, len(classes))
	for _, n := range classes {
		sizes = append(sizes, n)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(sizes)))
	top := sizes
	if len(top) > 10 {
		top = top[:10]
	}
	singletons := 0
	for _, n := range sizes {
		if n == 1 {
			singletons++
		}
	}
	t.Logf("[%s] n=%d outcomes: %s", group, len(feats), formatCounts(outcomeCounts))
	t.Logf("[%s] distinct coarse classes=%d singletons=%d top class sizes=%v", group, len(classes), singletons, top)
}

func formatCounts(m map[string]int) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", k, m[k]))
	}
	return strings.Join(parts, " ")
}
