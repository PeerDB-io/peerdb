//go:build ddlfuzz

package e2e

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Stats struct {
	stateDir string
	mu       sync.Mutex
	engines  map[string]*engineStats
}

type engineStats struct {
	Cases              uint64            `json:"cases"`
	ExecRejects        uint64            `json:"exec_rejects"`
	Findings           uint64            `json:"findings"`
	MatcherPending     int               `json:"matcher_pending"`
	MatcherLastEventAt int64             `json:"matcher_last_event_at"`
	Resets             uint64            `json:"resets"`
	QueueDone          uint64            `json:"queue_done"`
	MatchedDDLs        uint64            `json:"matched_ddls"`
	Markers            uint64            `json:"markers"`
	Controls           uint64            `json:"controls"`
	PaletteHits        map[string]uint64 `json:"palette_hits,omitempty"`
	GeneratorPanic     bool              `json:"generator_panic,omitempty"`
}

func NewStats(stateDir string, engines []engineConfig) *Stats {
	s := &Stats{stateDir: stateDir, engines: make(map[string]*engineStats, len(engines))}
	for _, ec := range engines {
		s.engines[ec.Name] = &engineStats{PaletteHits: make(map[string]uint64)}
	}
	return s
}

func (s *Stats) IncCases(engine string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.engines[engine].Cases++
}

func (s *Stats) IncExecReject(engine string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.engines[engine].ExecRejects++
}

func (s *Stats) IncFinding(engine string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.engines[engine].Findings++
}

func (s *Stats) IncMatchedDDL(engine string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	es := s.engines[engine]
	es.MatchedDDLs++
	es.MatcherLastEventAt = time.Now().Unix()
}

func (s *Stats) IncMarker(engine string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	es := s.engines[engine]
	es.Markers++
	es.MatcherLastEventAt = time.Now().Unix()
}

func (s *Stats) IncControl(engine string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	es := s.engines[engine]
	es.Controls++
	es.MatcherLastEventAt = time.Now().Unix()
}

func (s *Stats) IncReset(engine string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.engines[engine].Resets++
}

func (s *Stats) IncQueueDone(engine string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.engines[engine].QueueDone++
}

func (s *Stats) HitPalette(engine, mode string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.engines[engine].PaletteHits[mode]++
}

func (s *Stats) MarkGeneratorPanic(engine string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.engines[engine].GeneratorPanic = true
}

func (s *Stats) SetPending(engine string, pending int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.engines[engine].MatcherPending = pending
}

func (s *Stats) Snapshot() map[string]engineStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]engineStats, len(s.engines))
	for engine, es := range s.engines {
		cp := *es
		cp.PaletteHits = make(map[string]uint64, len(es.PaletteHits))
		for k, v := range es.PaletteHits {
			cp.PaletteHits[k] = v
		}
		out[engine] = cp
	}
	return out
}

func (s *Stats) Write() error {
	body := struct {
		UpdatedAt int64                  `json:"updated_at"`
		Engines   map[string]engineStats `json:"engines"`
	}{
		UpdatedAt: time.Now().Unix(),
		Engines:   s.Snapshot(),
	}
	return writeJSONAtomic(filepath.Join(s.stateDir, "e2e-stats.json"), body)
}

func (s *Stats) Summary(start time.Time) Summary {
	elapsed := time.Since(start)
	snap := s.Snapshot()
	out := Summary{StartedAt: start, Elapsed: elapsed, Engines: make(map[string]EngineSummary, len(snap))}
	secs := elapsed.Seconds()
	if secs <= 0 {
		secs = 1
	}
	for engine, es := range snap {
		out.Engines[engine] = EngineSummary{
			Cases:          es.Cases,
			ExecRejects:    es.ExecRejects,
			Findings:       es.Findings,
			MatchedDDLs:    es.MatchedDDLs,
			Markers:        es.Markers,
			Controls:       es.Controls,
			Resets:         es.Resets,
			QueueDone:      es.QueueDone,
			CasesPerSecond: float64(es.Cases) / secs,
			PaletteHits:    es.PaletteHits,
			GeneratorPanic: es.GeneratorPanic,
		}
	}
	return out
}

func writeJSONAtomic(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
