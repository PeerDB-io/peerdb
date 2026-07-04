package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/compare"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/corpus"
	ddllexec "github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/exec"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/findings"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/gen"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/mutate"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/oracle"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/run"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/sancov"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/seed"
)

const (
	recentRingSize    = 8192
	basePoolByteCap   = 256 << 20
	maxBaseEvictTries = 64
)

// fuzzLoop is the differential fast lane: generated/mutated cases go through
// the parser under test and the engine's parse oracle, divergences are
// classified by internal/compare and recorded as findings, and corpus
// retention is driven by oracle SanCov growth (plan 21 steps 3, 5, 10, 11).
// engineState bundles the per-engine coverage accumulator and mutation-base
// corpus behind their own mutex. Coverage merges (a scan of the multi-hundred-
// MB oracle bitmap) run under this lock, not the global statsMu, so the two
// engines merge in parallel and stats reads never block a merge.
type engineState struct {
	mu             sync.Mutex
	accum          *sancov.Accumulator
	bases          []run.Case
	baseKeys       map[string]struct{}
	basesBytes     int64
	recent         [recentRingSize]run.Case
	recentPos      int
	recentN        int
	recentKeys     map[string]int
	retainedByTier [3]uint64
}

type fuzzLoop struct {
	cfg   config
	store *corpus.Store

	engines map[string]*engineState

	statsMu        sync.Mutex
	openFindings   int
	suppressed     uint64
	findingsTotal  uint64
	classCounts    map[string]uint64
	oracleRestarts map[string]uint64

	execsTotal atomic.Uint64
	recordMu   sync.Mutex
	seedValue  uint64
}

type oracleProc struct {
	loop        *fuzzLoop
	engine      uint8
	engineName  string
	binPath     string
	id          int
	client      *oracle.Client
	worker      *ddllexec.Worker
	covWindow   []run.Case
	batches     int
	pollEvery   int
	emptyPolls  int
	logPath     string
	spawnErrors int
}

func (es *engineState) appendBaseLocked(c run.Case, rng *rand.Rand) {
	k := baseKey(c)
	if _, ok := es.baseKeys[k]; ok {
		return
	}
	es.bases = append(es.bases, c)
	es.baseKeys[k] = struct{}{}
	es.basesBytes += int64(len(c.SQL))
	es.evictBasesLocked(rng)
}

func (es *engineState) evictBasesLocked(rng *rand.Rand) {
	if rng == nil {
		rng = rand.New(rand.NewPCG(5, 6))
	}
	for es.basesBytes > basePoolByteCap && len(es.bases) > 0 {
		idx := -1
		for i := 0; i < maxBaseEvictTries; i++ {
			cand := rng.IntN(len(es.bases))
			if es.recentKeys[baseKey(es.bases[cand])] == 0 {
				idx = cand
				break
			}
		}
		if idx < 0 {
			for i, c := range es.bases {
				if es.recentKeys[baseKey(c)] == 0 {
					idx = i
					break
				}
			}
		}
		if idx < 0 {
			return
		}
		delete(es.baseKeys, baseKey(es.bases[idx]))
		es.basesBytes -= int64(len(es.bases[idx].SQL))
		last := len(es.bases) - 1
		es.bases[idx] = es.bases[last]
		es.bases = es.bases[:last]
	}
}

func (es *engineState) pushRecentLocked(c run.Case) {
	if es.recentN == len(es.recent) {
		old := es.recent[es.recentPos]
		k := baseKey(old)
		if es.recentKeys[k] <= 1 {
			delete(es.recentKeys, k)
		} else {
			es.recentKeys[k]--
		}
	} else {
		es.recentN++
	}
	es.recent[es.recentPos] = c
	es.recentKeys[baseKey(c)]++
	es.recentPos = (es.recentPos + 1) % len(es.recent)
}

func baseKey(c run.Case) string {
	return string(c.SQL) + "\x00" + fmt.Sprint(c.SQLMode)
}

func runFuzzLoop(ctx context.Context, cfg config) int {
	if err := os.MkdirAll(cfg.stateDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "fuzz: %v\n", err)
		return 1
	}
	seedValue, err := loadOrInitSeed(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fuzz: %v\n", err)
		return 1
	}
	if err := requireOracles(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "fuzz: %v\n", err)
		return 1
	}
	store, err := corpus.Open(filepath.Join(cfg.stateDir, "corpus.db"), cfg.corpusBudget)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fuzz: corpus: %v\n", err)
		return 1
	}
	defer store.Close()
	loop := &fuzzLoop{
		cfg:            cfg,
		store:          store,
		engines:        map[string]*engineState{},
		classCounts:    map[string]uint64{},
		oracleRestarts: map[string]uint64{},
		seedValue:      seedValue,
	}
	for _, engine := range []string{"mysql", "mariadb"} {
		acc, err := sancov.Load(filepath.Join(cfg.stateDir, "coverage", engine+".sancov"), engine)
		if err != nil {
			fmt.Fprintf(os.Stderr, "fuzz: coverage: %v\n", err)
			return 1
		}
		loop.engines[engine] = &engineState{accum: acc, baseKeys: map[string]struct{}{}, recentKeys: map[string]int{}}
	}
	if err := loop.loadCorpusBases(); err != nil {
		fmt.Fprintf(os.Stderr, "fuzz: corpus reload: %v\n", err)
		return 1
	}
	seeds, _ := seed.LoadDir(cfg.seedsDir)
	loop.loadSeedBases(seeds)
	loop.openFindings = countOpenFindings(cfg.stateDir)

	if cfg.duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.duration)
		defer cancel()
	}

	channels := map[uint8]chan []run.Case{
		run.EngineMySQL:   make(chan []run.Case, 4*cfg.oracleProcsPerEngine),
		run.EngineMariaDB: make(chan []run.Case, 4*cfg.oracleProcsPerEngine),
	}
	var wg sync.WaitGroup
	fatal := make(chan error, 2*cfg.oracleProcsPerEngine)
	for i := range cfg.oracleProcsPerEngine {
		for engine, ch := range channels {
			proc := &oracleProc{
				loop:       loop,
				engine:     engine,
				engineName: run.EngineName(engine),
				binPath:    cfg.oracleBin(engine),
				id:         i,
				worker:     ddllexec.NewWorker(i, cfg.caseDeadline, nil),
				pollEvery:  1,
				logPath:    filepath.Join(cfg.stateDir, "log", fmt.Sprintf("oracle-%s-%d.log", run.EngineName(engine), i)),
			}
			wg.Add(1)
			go func(ch chan []run.Case) {
				defer wg.Done()
				if err := proc.run(ctx, ch); err != nil {
					fatal <- err
				}
			}(ch)
		}
	}
	for i := range cfg.genWorkers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			loop.produce(ctx, workerID, channels)
		}(i)
	}

	exitCode := 0
	statsTicker := time.NewTicker(cfg.statsInterval)
	heartbeat := time.NewTicker(30 * time.Second)
	defer statsTicker.Stop()
	defer heartbeat.Stop()
	start := time.Now()
	last, lastTotal := start, uint64(0)
	loop.writeStats(0)
loopFor:
	for {
		select {
		case <-ctx.Done():
			break loopFor
		case err := <-fatal:
			fmt.Fprintf(os.Stderr, "fuzz: fatal: %v\n", err)
			exitCode = 3
			break loopFor
		case <-statsTicker.C:
			now := time.Now()
			cur := loop.execsTotal.Load()
			rate := float64(cur-lastTotal) / now.Sub(last).Seconds()
			loop.printStatsLine(rate, start)
			last, lastTotal = now, cur
		case <-heartbeat.C:
			loop.writeStats(loop.rate(start))
			loop.saveCoverage()
		}
	}
	wg.Wait()
	loop.writeStats(loop.rate(start))
	loop.saveCoverage()
	return exitCode
}

func (cfg config) oracleBin(engine uint8) string {
	if engine == run.EngineMariaDB {
		return cfg.mariaOracle
	}
	return cfg.mysqlOracle
}

func (l *fuzzLoop) loadSeedBases(seeds []seed.Seed) {
	for _, s := range seeds {
		engines := []uint8{run.EngineMySQL, run.EngineMariaDB}
		switch s.Engine {
		case "mysql":
			engines = engines[:1]
		case "mariadb", "maria":
			engines = engines[1:]
		}
		for _, engine := range engines {
			c := run.Case{SQL: []byte(s.SQL), SQLMode: s.SQLMode, Engine: engine, Origin: run.OriginCorpus}
			es := l.engines[run.EngineName(engine)]
			es.mu.Lock()
			es.appendBaseLocked(c, nil)
			es.mu.Unlock()
			_, _ = l.store.Add(c)
		}
	}
}

func (l *fuzzLoop) loadCorpusBases() error {
	rng := rand.New(rand.NewPCG(l.seedValue^0xfeed, 0xc0ffee))
	for _, engine := range []uint8{run.EngineMySQL, run.EngineMariaDB} {
		cases, err := l.store.AllCases(run.EngineName(engine))
		if err != nil {
			return err
		}
		es := l.engines[run.EngineName(engine)]
		es.mu.Lock()
		for _, c := range cases {
			es.appendBaseLocked(c, rng)
		}
		es.mu.Unlock()
	}
	return nil
}

func (l *fuzzLoop) produce(ctx context.Context, workerID int, channels map[uint8]chan []run.Case) {
	rng := rand.New(rand.NewPCG(l.seedValue, uint64(workerID)))
	for ctx.Err() == nil {
		engine := run.EngineMySQL
		if rng.Float64() >= l.cfg.engineBias {
			engine = run.EngineMariaDB
		}
		batch := make([]run.Case, 0, l.cfg.batch)
		for len(batch) < l.cfg.batch {
			batch = append(batch, l.nextCase(rng, engine))
		}
		select {
		case <-ctx.Done():
			return
		case channels[engine] <- batch:
		}
	}
}

func (l *fuzzLoop) nextCase(rng *rand.Rand, engine uint8) run.Case {
	es := l.engines[run.EngineName(engine)]
	es.mu.Lock()
	var base run.Case
	haveBase := false
	baseTier := run.BaseTierOld
	if len(es.bases) > 0 && rng.Float64() < l.cfg.mutRatio {
		if es.recentN > 0 && rng.IntN(2) == 0 {
			base = es.recent[rng.IntN(es.recentN)]
			baseTier = run.BaseTierRecent
		} else {
			base = es.bases[rng.IntN(len(es.bases))]
			baseTier = run.BaseTierOld
		}
		haveBase = true
	}
	es.mu.Unlock()
	if haveBase {
		c := mutate.Mutate(rng, base)
		c.Engine = engine
		c.Origin = run.OriginMut
		c.Seed = rng.Uint64()
		c.BaseTier = baseTier
		return c
	}
	mode := gen.ChooseMode(rng, engine == run.EngineMariaDB)
	sql := gen.Generate(rng, engine == run.EngineMariaDB, mode)
	return run.Case{
		SQL:      []byte(sql),
		SQLMode:  mode,
		Engine:   engine,
		Origin:   run.OriginGen,
		Seed:     rng.Uint64(),
		BaseTier: run.BaseTierFreshGen,
	}
}

func (p *oracleProc) run(ctx context.Context, ch chan []run.Case) error {
	defer func() {
		if p.client != nil {
			_ = p.client.Close()
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return nil
		case batch := <-ch:
			if err := p.ensureClient(ctx); err != nil {
				return err
			}
			p.processBatch(ctx, batch, 0)
			if ctx.Err() != nil {
				return nil
			}
			p.batches++
			if p.batches >= p.pollEvery {
				p.batches = 0
				p.pollCoverage(ctx)
			}
		}
	}
}

func (p *oracleProc) ensureClient(ctx context.Context) error {
	if p.client != nil {
		return nil
	}
	backoff := 100 * time.Millisecond
	for {
		client := oracle.NewClient(p.engineName, p.binPath, p.loop.cfg.oracleBatchTimeout)
		err := client.Start(ctx, p.logPath)
		if err == nil {
			p.client = client
			p.spawnErrors = 0
			return nil
		}
		p.spawnErrors++
		if p.spawnErrors > 20 {
			return fmt.Errorf("oracle %s: %d consecutive spawn failures: %w", p.engineName, p.spawnErrors, err)
		}
		if ctx.Err() != nil {
			return nil
		}
		time.Sleep(backoff)
		if backoff *= 2; backoff > 5*time.Second {
			backoff = 5 * time.Second
		}
	}
}

func (p *oracleProc) restartClient(ctx context.Context) error {
	if p.client != nil {
		_ = p.client.Close()
		p.client = nil
	}
	p.loop.statsMu.Lock()
	p.loop.oracleRestarts[p.engineName]++
	p.loop.statsMu.Unlock()
	return p.ensureClient(ctx)
}

// processBatch runs one batch through parser + oracle. On oracle failure the
// batch is bisected recursively to isolate the crashing/hanging input (plan 21
// step 3); depth caps the resubmissions.
func (p *oracleProc) processBatch(ctx context.Context, batch []run.Case, depth int) {
	if len(batch) == 0 || ctx.Err() != nil {
		return
	}
	results := p.worker.RunBatch(batch)
	digests, _, err := p.client.ParseBatch(ctx, batch)
	if err == nil && len(digests) == len(batch) {
		for i, c := range batch {
			div := compare.Diff(c, results[i].Sig, results[i].Err, results[i].Panic, digests[i])
			if div != nil {
				p.loop.recordFinding(findings.FindingFromDivergence(c, div, oracle.RawDigestJSON(digests[i])))
			}
		}
		p.covWindow = append(p.covWindow, batch...)
		p.loop.execsTotal.Add(uint64(len(batch)))
		return
	}
	if ctx.Err() != nil {
		return
	}
	if restartErr := p.restartClient(ctx); restartErr != nil || p.client == nil {
		return
	}
	class := "oracle_crash"
	if errors.Is(err, context.DeadlineExceeded) {
		class = "oracle_timeout"
	}
	if len(batch) == 1 {
		c := batch[0]
		p.loop.recordFinding(findings.Finding{
			Class:     class,
			Engine:    p.engineName,
			SQLMode:   c.SQLMode,
			Lane:      "fast",
			Statement: c.SQL,
			Meta: map[string]any{
				"shape":  "head=" + compare.HeadWord(c.SQL),
				"origin": run.OriginName(c.Origin),
				"error":  errText(err),
			},
		})
		p.loop.execsTotal.Add(1)
		return
	}
	if depth >= 2*bitLen(len(batch)) {
		p.recordBatchFinding(class, batch, err)
		return
	}
	mid := len(batch) / 2
	p.processBatch(ctx, batch[:mid], depth+1)
	p.processBatch(ctx, batch[mid:], depth+1)
}

// recordBatchFinding files a state-dependent oracle crash that no single input
// reproduces: the whole batch is the repro, NUL-line separated per plan 21.
func (p *oracleProc) recordBatchFinding(class string, batch []run.Case, err error) {
	var repro []byte
	for i, c := range batch {
		if i > 0 {
			repro = append(repro, '\n', 0, '\n')
		}
		repro = append(repro, c.SQL...)
	}
	p.loop.recordFinding(findings.Finding{
		Class:     class,
		Engine:    p.engineName,
		SQLMode:   batch[0].SQLMode,
		Lane:      "fast",
		Statement: repro,
		Meta: map[string]any{
			"shape":  "state-dependent batch",
			"batch":  true,
			"origin": run.OriginName(batch[0].Origin),
			"error":  errText(err),
		},
	})
}

func (p *oracleProc) pollCoverage(ctx context.Context) {
	if p.client == nil || len(p.covWindow) == 0 {
		return
	}
	counters, err := p.client.Coverage(ctx)
	if err != nil {
		_ = p.restartClient(ctx)
		p.covWindow = p.covWindow[:0]
		return
	}
	es := p.loop.engines[p.engineName]
	es.mu.Lock()
	grew, _ := es.accum.Merge(counters)
	es.mu.Unlock()
	if grew {
		// Attribution is window-level guesswork, so keep a bounded sample
		// instead of the whole window (late-run windows reach pollEvery=32
		// batches; unbounded retention is what blows up the corpus). Prefer
		// the smallest inputs — they mutate better.
		window := p.covWindow
		if max := p.loop.cfg.retainPerPoll; max > 0 && len(window) > max {
			window = append([]run.Case(nil), window...)
			sort.SliceStable(window, func(i, j int) bool {
				return len(window[i].SQL) < len(window[j].SQL)
			})
			window = window[:max]
		}
		var added []run.Case
		for _, c := range window {
			if ok, err := p.loop.store.Add(c); err == nil && ok {
				added = append(added, c)
			}
		}
		es.mu.Lock()
		for _, c := range added {
			es.pushRecentLocked(c)
			es.appendBaseLocked(c, rand.New(rand.NewPCG(uint64(len(c.SQL))+c.SQLMode, uint64(c.Engine)+1)))
			if c.BaseTier < uint8(len(es.retainedByTier)) {
				es.retainedByTier[c.BaseTier]++
			}
		}
		es.mu.Unlock()
		p.emptyPolls = 0
		p.pollEvery = 1
	} else {
		p.emptyPolls++
		if p.emptyPolls >= 16 && p.pollEvery < 32 {
			p.pollEvery *= 2
			p.emptyPolls = 0
		}
	}
	p.covWindow = p.covWindow[:0]
}

func (l *fuzzLoop) recordFinding(f findings.Finding) {
	l.recordMu.Lock()
	defer l.recordMu.Unlock()
	if l.openFindings >= l.cfg.maxOpenFindings {
		l.statsMu.Lock()
		l.suppressed++
		l.statsMu.Unlock()
		return
	}
	sig, isNew, err := findings.Record(l.cfg.stateDir, f)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fuzz: record finding: %v\n", err)
		return
	}
	_ = sig
	l.statsMu.Lock()
	if isNew {
		l.openFindings++
		l.findingsTotal++
		l.classCounts[f.Class]++
	} else {
		// re-seen sig or parked/ledgered suppression: dedup layer absorbed it
		l.suppressed++
	}
	l.statsMu.Unlock()
}

func (l *fuzzLoop) rate(start time.Time) float64 {
	elapsed := time.Since(start).Seconds()
	if elapsed <= 0 {
		return 0
	}
	return float64(l.execsTotal.Load()) / elapsed
}

func (l *fuzzLoop) writeStats(rate float64) {
	l.statsMu.Lock()
	stats := map[string]any{
		"ts":            time.Now().UTC().Format(time.RFC3339),
		"execs_total":   l.execsTotal.Load(),
		"execs_per_sec": rate,
		"corpus_count": map[string]int{
			"mysql":   l.store.Count("mysql"),
			"mariadb": l.store.Count("mariadb"),
		},
		"edges": map[string]int{
			"go":      0,
			"mysql":   l.edgeCount("mysql"),
			"mariadb": l.edgeCount("mariadb"),
		},
		"oracle_restarts": map[string]uint64{
			"mysql":   l.oracleRestarts["mysql"],
			"mariadb": l.oracleRestarts["mariadb"],
		},
		"findings_emitted_total": l.findingsTotal,
		"run_seed":               fmt.Sprintf("0x%x", l.seedValue),
		"class_counts":           copyCounts(l.classCounts),
		"suppressed":             l.suppressed,
		"corpus_bytes":           l.store.Bytes(),
		"retention_skipped":      l.store.SkippedFull(),
	}
	stats["retained_by_tier"] = l.retainedByTierStats()
	stats["bases_bytes"] = l.basesBytesStats()
	l.statsMu.Unlock()
	_ = writeStatsJSON(l.cfg.stateDir, stats)
}

func (l *fuzzLoop) retainedByTierStats() map[string]map[string]uint64 {
	out := map[string]map[string]uint64{}
	for _, engine := range []string{"mysql", "mariadb"} {
		es := l.engines[engine]
		es.mu.Lock()
		out[engine] = map[string]uint64{
			"fresh_gen":   es.retainedByTier[run.BaseTierFreshGen],
			"recent_base": es.retainedByTier[run.BaseTierRecent],
			"old_base":    es.retainedByTier[run.BaseTierOld],
		}
		es.mu.Unlock()
	}
	return out
}

func (l *fuzzLoop) basesBytesStats() map[string]int64 {
	out := map[string]int64{}
	for _, engine := range []string{"mysql", "mariadb"} {
		es := l.engines[engine]
		es.mu.Lock()
		out[engine] = es.basesBytes
		es.mu.Unlock()
	}
	return out
}

func (l *fuzzLoop) edgeCount(engine string) int {
	es := l.engines[engine]
	es.mu.Lock()
	defer es.mu.Unlock()
	return es.accum.EdgeCount()
}

func (l *fuzzLoop) printStatsLine(rate float64, start time.Time) {
	l.statsMu.Lock()
	open, supp := l.openFindings, l.suppressed
	myRestarts, maRestarts := l.oracleRestarts["mysql"], l.oracleRestarts["mariadb"]
	l.statsMu.Unlock()
	fmt.Fprintf(os.Stderr, "execs/s=%.0f corpus=mysql:%d,maria:%d edges=my:%d,ma:%d findings=open:%d,supp:%d restarts=my:%d,ma:%d uptime=%s\n",
		rate, l.store.Count("mysql"), l.store.Count("mariadb"),
		l.edgeCount("mysql"), l.edgeCount("mariadb"),
		open, supp, myRestarts, maRestarts,
		time.Since(start).Round(time.Second))
}

func (l *fuzzLoop) saveCoverage() {
	for engine, es := range l.engines {
		path := filepath.Join(l.cfg.stateDir, "coverage", engine+".sancov")
		es.mu.Lock()
		err := es.accum.Save(path)
		es.mu.Unlock()
		if err != nil {
			fmt.Fprintf(os.Stderr, "fuzz: save coverage: %v\n", err)
		}
	}
}

func countOpenFindings(stateDir string) int {
	entries, err := os.ReadDir(filepath.Join(stateDir, "findings"))
	if err != nil {
		return 0
	}
	open := 0
	for _, ent := range entries {
		if !ent.IsDir() || len(ent.Name()) != 12 {
			continue
		}
		b, err := os.ReadFile(filepath.Join(stateDir, "findings", ent.Name(), "meta.json"))
		if err != nil {
			continue
		}
		var meta struct {
			Status string `json:"status"`
		}
		if json.Unmarshal(b, &meta) == nil && (meta.Status == "" || meta.Status == "open") {
			open++
		}
	}
	return open
}

func copyCounts(m map[string]uint64) map[string]uint64 {
	out := make(map[string]uint64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func bitLen(n int) int {
	b := 0
	for n > 0 {
		b++
		n >>= 1
	}
	return b
}
