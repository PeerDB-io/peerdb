package fuzzcmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/compare"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/corpus"
	ddllexec "github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/exec"
	"github.com/PeerDB-io/peerdb/tools/ddlfuzz/internal/run"
)

// distillFeatureKey re-parses a stored row with the in-process parser and
// reduces the outcome to its behavior feature fold. Offline there is no oracle
// digest, so only the our-side feature set contributes; the fold is
// process-stable, so stamped features from different distill runs stay
// comparable.
func distillFeatureKey(caseDeadline time.Duration) corpus.FeatureKeyFunc {
	worker := ddllexec.NewWorker(0, caseDeadline, nil)
	return func(engine uint8, sqlMode uint64, sqlText []byte) uint64 {
		c := run.Case{SQL: sqlText, SQLMode: sqlMode, Engine: engine}
		res := worker.RunBatch([]run.Case{c})[0]
		return compare.BehaviorFeature(c, res.Sig, res.Err, res.Panic, nil)
	}
}

func runCorpusDistill(cfg config) int {
	if (cfg.behaviorSince == "") != (cfg.behaviorUntil == "") {
		fmt.Fprintln(os.Stderr, "corpus-distill: -behavior-since and -behavior-until must be passed together")
		return 2
	}
	store, err := corpus.Open(filepath.Join(cfg.stateDir, "corpus.db"), 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "corpus-distill: %v\n", err)
		return 1
	}
	defer store.Close()
	keyFn := distillFeatureKey(cfg.caseDeadline)
	var stats corpus.DistillStats
	if cfg.behaviorSince != "" {
		since, err := time.Parse(time.RFC3339, cfg.behaviorSince)
		if err != nil {
			fmt.Fprintf(os.Stderr, "corpus-distill: behavior-since: %v\n", err)
			return 2
		}
		until, err := time.Parse(time.RFC3339, cfg.behaviorUntil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "corpus-distill: behavior-until: %v\n", err)
			return 2
		}
		if !since.Before(until) {
			fmt.Fprintln(os.Stderr, "corpus-distill: behavior-since must be before behavior-until")
			return 2
		}
		stats, err = store.DistillBehaviorWindow(keyFn, since, until)
	} else {
		stats, err = store.DistillNoise(keyFn)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "corpus-distill: %v\n", err)
		return 1
	}
	if err := store.Vacuum(); err != nil {
		fmt.Fprintf(os.Stderr, "corpus-distill vacuum: %v\n", err)
		return 1
	}
	b, _ := json.Marshal(stats)
	fmt.Fprintln(os.Stdout, string(b))
	return 0
}
