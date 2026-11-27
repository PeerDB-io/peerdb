package otel_metrics

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/PeerDB-io/peerdb/flow/shared"
)

const droppedFlowsSweepInterval = time.Hour

// A flow's series are deleted if it was last seen as a live flow more than exportWatermarkBuffer before
// the start of the last successful metric export.
const exportWatermarkBuffer = 5 * time.Minute

// clockBase anchors monotonicNanos.
var clockBase = time.Now()

func monotonicNanos() int64 {
	return int64(time.Since(clockBase))
}

// shouldDelete reports whether a series has a flowName that keep rejects. Series without one are always kept.
func shouldDelete(attrs attribute.Set, keep func(flowName string) bool) bool {
	flowName, ok := attrs.Value(FlowNameKey)
	return ok && !keep(flowName.AsString())
}

type sweepableInstrument interface {
	// sweep deletes the series whose flows keep rejects and returns how many it deleted.
	sweep(keep func(flowName string) bool) int
}

// seriesSweeper deletes the series of dropped flows from every instrument of an OtelManager.
type seriesSweeper struct {
	// droppedFlows maps flows missing from the catalog to when they were first seen missing, in monotonicNanos.
	// Only used by sweep, which runs on one goroutine, so it needs no synchronization.
	droppedFlows map[string]int64
	mu           struct {
		instruments []sweepableInstrument
		sync.Mutex
	}
	exportWatermark atomic.Int64
}

func (s *seriesSweeper) register(instrument sweepableInstrument) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.instruments = append(s.mu.instruments, instrument)
}

// sweeperOption makes a NewContextAware* constructor register its instrument with sweeper. The metric option
// interfaces have unexported methods, so it embeds them to satisfy them; the embedded values are nil, and the
// constructors remove the option with takeSweeper before building their config, so they are never called.
type sweeperOption struct {
	metric.Int64CounterOption
	metric.Int64GaugeOption
	metric.Float64GaugeOption
	metric.Int64HistogramOption
	sweeper *seriesSweeper
}

func withSweeper(sweeper *seriesSweeper) sweeperOption {
	return sweeperOption{sweeper: sweeper}
}

// takeSweeper returns the sweeper passed with withSweeper, or nil, and the other options.
func takeSweeper[O any](opts []O) (*seriesSweeper, []O) {
	var sweeper *seriesSweeper
	rest := make([]O, 0, len(opts))
	for _, opt := range opts {
		if option, ok := any(opt).(sweeperOption); ok {
			sweeper = option.sweeper
			continue
		}
		rest = append(rest, opt)
	}
	return sweeper, rest
}

// sweep deletes the series of flows not in liveFlows that were already missing at an earlier sweep, once an export
// has started exportWatermarkBuffer after that. Only safe to call from one goroutine, see runSweepLoop.
func (s *seriesSweeper) sweep(liveFlows []string) int {
	live := make(map[string]struct{}, len(liveFlows))
	for _, name := range liveFlows {
		live[name] = struct{}{}
	}
	exportWatermark := s.exportWatermark.Load()

	s.mu.Lock()
	instruments := s.mu.instruments
	s.mu.Unlock()
	now := monotonicNanos()
	toDelete := make(map[string]struct{})
	keep := func(flowName string) bool {
		if flowName == "" {
			return true
		}
		if _, ok := live[flowName]; ok {
			// A recreated flow must start its grace period over when it is dropped again.
			delete(s.droppedFlows, flowName)
			return true
		}
		missingSince, ok := s.droppedFlows[flowName]
		if !ok {
			// Delete on a later sweep, once an export has started exportWatermarkBuffer after now.
			s.droppedFlows[flowName] = now
			return true
		}
		if exportWatermark-int64(exportWatermarkBuffer) > missingSince {
			toDelete[flowName] = struct{}{}
			return false
		}
		return true
	}
	deleted := 0
	for _, instrument := range instruments {
		deleted += instrument.sweep(keep)
	}
	for key := range toDelete {
		delete(s.droppedFlows, key)
	}
	return deleted
}

func (s *seriesSweeper) runSweepLoop(
	ctx context.Context,
	catalogPool *shared.CatalogPool,
	interval time.Duration,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}

		rows, err := catalogPool.Query(ctx, "SELECT name FROM flows")
		var flowNames []string
		if err == nil {
			flowNames, err = pgx.CollectRows(rows, pgx.RowTo[string])
		}
		if err != nil {
			if ctx.Err() == nil {
				slog.WarnContext(ctx, "failed to list flows for sweeping metric series", slog.Any("error", err))
			}
			continue
		}
		if deleted := s.sweep(flowNames); deleted > 0 {
			slog.InfoContext(ctx, "deleted metric series of dropped flows", slog.Int("series", deleted))
		}
	}
}

// exportWatermarkExporter sets the export watermark to the start of each successful export.
type exportWatermarkExporter struct {
	sdkmetric.Exporter
	watermark *atomic.Int64
}

func (e *exportWatermarkExporter) Export(ctx context.Context, rm *metricdata.ResourceMetrics) error {
	start := monotonicNanos()
	if err := e.Exporter.Export(ctx, rm); err != nil {
		return err
	}
	e.watermark.Store(start)
	return nil
}
