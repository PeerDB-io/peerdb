package otel_metrics

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/PeerDB-io/peerdb/flow/shared"
)

const (
	droppedFlowsSweepInterval = time.Hour
	// droppedFlowsDeliveryBuffer is how long after detection an export must start before a dropped flow's series
	// are deleted. It covers values recorded shortly after the flow's catalog row was deleted, for example by
	// scheduled tasks that listed flows just before, as well as the window between collection and export start.
	droppedFlowsDeliveryBuffer = 5 * time.Minute
)

type sweepableInstrument interface {
	// collectDroppedFlows adds to dst the flow names of series or cached entries belonging to flows not in live.
	collectDroppedFlows(live map[string]struct{}, dst map[string]struct{})
	// deleteFlows removes series for flows in toDelete, clears cached entries, and returns how many series were deleted.
	deleteFlows(toDelete map[string]struct{}) int
}

// droppedFlowsSweeper deletes the series and cached context entries of flows deleted from the catalog, once an
// export that started at least droppedFlowsDeliveryBuffer after their detection succeeded. Each process sweeps
// its own instruments, since a flow's series live in every process that ran its activities.
type droppedFlowsSweeper struct {
	// lastDeliveredAt is the start of the latest successful export. Values recorded before it were exported,
	// since the reader collects just before exporting.
	lastDeliveredAt atomic.Pointer[time.Time]
	// detectedAt is when a sweep first found each dropped flow whose series are not deleted yet. Only the sweep
	// goroutine uses it.
	detectedAt map[string]time.Time
	cancel     context.CancelFunc
	// instruments are set by start; immutable once the sweep loop runs.
	instruments []sweepableInstrument
	wg          sync.WaitGroup
}

// start runs the periodic sweep loop over instruments in the background until stop is called or ctx is done.
func (s *droppedFlowsSweeper) start(ctx context.Context, catalogPool *shared.CatalogPool, instruments []sweepableInstrument) {
	if s == nil {
		return
	}
	s.instruments = instruments
	var sweepCtx context.Context
	sweepCtx, s.cancel = context.WithCancel(ctx)
	s.wg.Go(func() {
		s.runSweepLoop(sweepCtx, catalogPool)
	})
}

// stop cancels the sweep loop and waits for it to exit. Safe to call on a nil or unstarted sweeper.
func (s *droppedFlowsSweeper) stop() {
	if s == nil || s.cancel == nil {
		return
	}
	s.cancel()
	s.wg.Wait()
}

// sweep reconciles registered instruments against liveFlows and returns how many series it deleted. now must be
// taken after the listing, so newly dropped flows get a detection timestamp after the catalog snapshot.
func (s *droppedFlowsSweeper) sweep(liveFlows []string, now time.Time) int {
	live := make(map[string]struct{}, len(liveFlows))
	for _, name := range liveFlows {
		live[name] = struct{}{}
	}

	dropped := make(map[string]struct{})
	for _, instrument := range s.instruments {
		instrument.collectDroppedFlows(live, dropped)
	}

	lastDeliveredAt := s.lastDeliveredAt.Load()
	// Rebuilt each sweep, so flows that are live again, were deleted, or no longer have state drop out.
	detectedAt := make(map[string]time.Time)
	var toDelete map[string]struct{}
	for flowName := range dropped {
		detected, ok := s.detectedAt[flowName]
		if !ok {
			detectedAt[flowName] = now
			continue
		}
		if lastDeliveredAt != nil && detected.Add(droppedFlowsDeliveryBuffer).Before(*lastDeliveredAt) {
			if toDelete == nil {
				toDelete = make(map[string]struct{})
			}
			toDelete[flowName] = struct{}{}
		} else {
			detectedAt[flowName] = detected
		}
	}
	s.detectedAt = detectedAt

	if len(toDelete) == 0 {
		return 0
	}
	deleted := 0
	for _, instrument := range s.instruments {
		deleted += instrument.deleteFlows(toDelete)
	}
	return deleted
}

// runSweepLoop sweeps every droppedFlowsSweepInterval until ctx is done. A failed listing skips that sweep.
func (s *droppedFlowsSweeper) runSweepLoop(ctx context.Context, catalogPool *shared.CatalogPool) {
	ticker := time.NewTicker(droppedFlowsSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		flowNames, err := listFlowNames(ctx, catalogPool)
		if err != nil {
			if ctx.Err() == nil {
				slog.WarnContext(ctx, "failed to list flows to sweep metric series of dropped flows", slog.Any("error", err))
			}
			continue
		}
		if deleted := s.sweep(flowNames, time.Now()); deleted > 0 {
			slog.InfoContext(ctx, "deleted metric series of dropped flows", slog.Int("series", deleted))
		}
	}
}

func listFlowNames(ctx context.Context, catalogPool *shared.CatalogPool) ([]string, error) {
	rows, err := catalogPool.Query(ctx, "SELECT name FROM flows")
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// wrapExporter wraps exporter to record successful exports in s.lastDeliveredAt. Safe to call on a nil sweeper.
func (s *droppedFlowsSweeper) wrapExporter(exporter sdkmetric.Exporter) sdkmetric.Exporter {
	if s == nil {
		return exporter
	}
	return &deliveryTrackingExporter{Exporter: exporter, lastDeliveredAt: &s.lastDeliveredAt}
}

// deliveryTrackingExporter records the start of each successful export in lastDeliveredAt.
type deliveryTrackingExporter struct {
	sdkmetric.Exporter
	lastDeliveredAt *atomic.Pointer[time.Time]
}

func (e *deliveryTrackingExporter) Export(ctx context.Context, rm *metricdata.ResourceMetrics) error {
	startedAt := time.Now()
	if err := e.Exporter.Export(ctx, rm); err != nil {
		return err
	}
	e.lastDeliveredAt.Store(&startedAt)
	return nil
}
