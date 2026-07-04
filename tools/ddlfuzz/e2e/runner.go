package e2e

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/corpus"
)

// Run starts the selected e2e engines and blocks until the configured case
// target is reached, ctx is canceled, or a harness error occurs.
func Run(ctx context.Context, cfg Config) (Summary, error) {
	if cfg.Workers <= 0 {
		cfg.Workers = 4
	}
	if len(cfg.Engines) == 0 {
		cfg.Engines = DefaultConfig().Engines
	}
	if cfg.MySQLAddr == "" {
		cfg.MySQLAddr = DefaultConfig().MySQLAddr
	}
	if cfg.MariaDBAddr == "" {
		cfg.MariaDBAddr = DefaultConfig().MariaDBAddr
	}
	stateDir, err := absStateDir(cfg.StateDir)
	if err != nil {
		return Summary{}, err
	}
	cfg.StateDir = stateDir
	if err := ensureStateLayout(cfg.StateDir); err != nil {
		return Summary{}, err
	}
	corpusStore, err := corpus.Open(filepath.Join(cfg.StateDir, "corpus.db"), 0)
	if err != nil {
		return Summary{}, fmt.Errorf("open corpus: %w", err)
	}
	defer corpusStore.Close()

	engines, err := parseEngineList(cfg)
	if err != nil {
		return Summary{}, err
	}

	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer cancel()

	stats := NewStats(cfg.StateDir, engines)
	sides := newSideChannels(cfg.StateDir)
	start := time.Now()
	errs := make(chan error, len(engines)+1)
	queueChans := make(map[string]chan<- queueItem, len(engines))
	var matchers []*matcher
	var wg sync.WaitGroup

	for _, ec := range engines {
		if err := setupEngineSchemas(ctx, ec, cfg.Workers); err != nil {
			return stats.Summary(start), fmt.Errorf("setup %s: %w", ec.Name, err)
		}
		queues := make(map[string]*expectQueue, cfg.Workers)
		for i := 1; i <= cfg.Workers; i++ {
			queues[schemaName(i)] = newExpectQueue()
		}
		m, err := startMatcher(ctx, ec, cfg.StateDir, queues, stats, sides, errs)
		if err != nil {
			return stats.Summary(start), fmt.Errorf("start matcher %s: %w", ec.Name, err)
		}
		matchers = append(matchers, m)
		rt := &engineRuntime{
			ec:        ec,
			stateDir:  cfg.StateDir,
			workers:   cfg.Workers,
			target:    cfg.Cases,
			smoke:     cfg.Smoke,
			runNonce:  fmt.Sprintf("%08x", uint32(time.Now().UnixNano())),
			stats:     stats,
			sides:     sides,
			errs:      errs,
			queues:    queues,
			queueChan: make(chan queueItem, 64),
			corpus:    corpusStore,
		}
		queueChans[ec.Name] = rt.queueChan
		for i := 1; i <= cfg.Workers; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				rt.runWorker(ctx, id)
			}(i)
		}
	}

	queueCtx, stopQueue := context.WithCancel(ctx)
	defer stopQueue()
	go pollQueue(queueCtx, cfg.StateDir, queueChans, stats)

	statsCtx, stopStats := context.WithCancel(ctx)
	defer stopStats()
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-statsCtx.Done():
				return
			case <-ticker.C:
				_ = stats.Write()
			}
		}
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	var runErr error
	select {
	case <-done:
	case err := <-errs:
		runErr = err
		cancel()
	case <-ctx.Done():
		runErr = ctx.Err()
	}

	stopQueue()
	stopStats()
	if runErr == nil {
		drainUntil := time.Now().Add(10 * time.Second)
		for time.Now().Before(drainUntil) {
			allEmpty := true
			for _, m := range matchers {
				for _, q := range m.queues {
					if q.Len() != 0 {
						allEmpty = false
					}
				}
			}
			if allEmpty {
				break
			}
			select {
			case err := <-errs:
				runErr = err
				cancel()
			case <-time.After(20 * time.Millisecond):
			}
			if runErr != nil {
				break
			}
		}
	}
	cancel()
	for _, m := range matchers {
		m.Close()
	}
	_ = stats.Write()
	sum := stats.Summary(start)
	if cfg.Smoke && runErr == nil {
		runErr = validateSmokeSummary(cfg, sum)
	}
	return sum, runErr
}

func validateSmokeSummary(cfg Config, sum Summary) error {
	for engine, es := range sum.Engines {
		if cfg.Cases > 0 && es.Cases < cfg.Cases {
			return fmt.Errorf("smoke %s: cases=%d want at least %d", engine, es.Cases, cfg.Cases)
		}
		if es.Markers < es.Cases {
			return fmt.Errorf("smoke %s: markers=%d cases=%d", engine, es.Markers, es.Cases)
		}
		if es.Cases != es.ExecRejects+es.MatchedDDLs {
			return fmt.Errorf("smoke %s: cases=%d rejects+matched=%d", engine, es.Cases, es.ExecRejects+es.MatchedDDLs)
		}
		palette := mysqlSQLModes
		if engine == EngineMariaDB {
			palette = mariaSQLModes
		}
		for _, entry := range uniqueStrings(palette) {
			if es.PaletteHits[entry] == 0 {
				return fmt.Errorf("smoke %s: palette entry %q was not hit", engine, entry)
			}
		}
	}
	return nil
}

func uniqueStrings(in []string) []string {
	seen := make(map[string]bool, len(in))
	var out []string
	for _, v := range in {
		if seen[v] {
			continue
		}
		seen[v] = true
		out = append(out, v)
	}
	return out
}
