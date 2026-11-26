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

// clockBase anchors monotonicNanos.
var clockBase = time.Now()

func monotonicNanos() int64 {
	return int64(time.Since(clockBase))
}

// seriesActivity records when a series was last updated, and whether a sweep deleted it.
type seriesActivity struct {
	lastUpdate atomic.Int64 // monotonicNanos; see touch
	deleted    atomic.Bool
}

// touch records an update. Call it after storing the value to update the watermark of when
// this value was last seen and needs to be exported.
// exportWatermark is the watermark loaded after storing the value.
func (a *seriesActivity) touch(exportWatermark int64) {
	last := a.lastUpdate.Load()
	if last >= exportWatermark {
		return
	}
	now := monotonicNanos()
	for {
		// Concurrent updates may record out of order; keep the latest.
		if last >= now || a.lastUpdate.CompareAndSwap(last, now) {
			return
		}
		last = a.lastUpdate.Load()
	}
}

// shouldDelete reports whether a series belongs to a flow that is not live and its latest value was exported.
func (a *seriesActivity) shouldDelete(attrs attribute.Set, isLive func(flowName string) bool, exportWatermark int64) bool {
	if a.lastUpdate.Load() >= exportWatermark {
		return false
	}
	flowName, ok := attrs.Value(FlowNameKey)
	return ok && !isLive(flowName.AsString())
}

func (a *seriesActivity) markDeleted() {
	a.deleted.Store(true)
}

func (a *seriesActivity) isDeleted() bool {
	return a.deleted.Load()
}

type sweepableInstrument interface {
	// sweep deletes the instrument's sweepable series and returns how many it deleted.
	sweep(isLive func(flowName string) bool, exportWatermark int64) int
}

// seriesSweeper deletes the series of dropped flows from every instrument of an OtelManager, once
// exportWatermarkExporter has advanced past their last update, so a dropped flow's last values are exported first.
type seriesSweeper struct {
	mu struct {
		instruments []sweepableInstrument
		sync.Mutex
	}
	// exportWatermark is the monotonicNanos start of the last successful export.
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

// exportWatermarkFor returns the watermark instruments record updates against: the sweeper's, or for instruments
// without one, a watermark that never advances, so their series are never deleted.
func exportWatermarkFor(sweeper *seriesSweeper) *atomic.Int64 {
	if sweeper == nil {
		return new(atomic.Int64)
	}
	return &sweeper.exportWatermark
}

// sweep deletes the series of flows not in liveFlows whose latest values were exported.
func (s *seriesSweeper) sweep(liveFlows []string) int {
	live := make(map[string]struct{}, len(liveFlows))
	for _, name := range liveFlows {
		live[name] = struct{}{}
	}
	isLive := func(flowName string) bool {
		_, ok := live[flowName]
		return ok
	}
	exportWatermark := s.exportWatermark.Load()

	s.mu.Lock()
	instruments := s.mu.instruments
	s.mu.Unlock()
	deleted := 0
	for _, instrument := range instruments {
		deleted += instrument.sweep(isLive, exportWatermark)
	}
	return deleted
}

// runSweepLoop sweeps every interval until ctx is done. A failed flow listing skips that sweep.
// Flows are listed before sweeping, and a flow's row is created before its workflow starts, so a mirror created
// after the listing has no series old enough to be deleted.
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

// exportWatermarkExporter sets the export watermark to the start of each successful export. The data was collected
// just before Export was called, so updates made between that collection and the start of Export are treated as
// exported although they are not; that window is accepted.
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
