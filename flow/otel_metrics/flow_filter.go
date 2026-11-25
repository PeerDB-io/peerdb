package otel_metrics

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const liveFlowsRefreshInterval = 5 * time.Minute

type FlowNamesLister func(ctx context.Context) ([]string, error)

type liveFlowsSnapshot struct {
	names   map[string]struct{}
	takenAt time.Time
}

// liveFlowsFilter drops exported data points whose flowName is not in the catalog, so dropped mirrors
// stop being exported by every process that ran them. It only filters the export; series stay in memory.
type liveFlowsFilter struct {
	snapshot atomic.Pointer[liveFlowsSnapshot]
	now      func() time.Time

	mu struct {
		// missingSince records when a flowName was first exported while absent from the snapshot. A flow is only
		// dropped once a snapshot taken after that time still lacks it.
		missingSince map[string]time.Time
		sync.Mutex
	}
}

func newLiveFlowsFilter() *liveFlowsFilter {
	lff := &liveFlowsFilter{now: time.Now}
	lff.mu.missingSince = make(map[string]time.Time)
	return lff
}

func (f *liveFlowsFilter) refresh(ctx context.Context, list FlowNamesLister) error {
	takenAt := f.now()
	flowNames, err := list(ctx)
	if err != nil {
		return err
	}
	names := make(map[string]struct{}, len(flowNames))
	for _, name := range flowNames {
		names[name] = struct{}{}
	}
	func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		for name := range f.mu.missingSince {
			if _, ok := names[name]; ok {
				delete(f.mu.missingSince, name)
			}
		}
	}()

	f.snapshot.Store(&liveFlowsSnapshot{names: names, takenAt: takenAt})
	return nil
}

// runRefreshLoop refreshes the snapshot until ctx is done. Until the first successful refresh nothing is filtered,
// and a failed refresh keeps the previous snapshot.
func (f *liveFlowsFilter) runRefreshLoop(ctx context.Context, list FlowNamesLister, interval time.Duration, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		if err := f.refresh(ctx, list); err != nil && ctx.Err() == nil {
			slog.WarnContext(ctx, "failed to refresh live flows for metrics filtering", slog.Any("error", err))
		}
		// jitter so processes don't query the catalog in lockstep
		wait := interval/2 + rand.N(interval) //nolint:gosec // jitter, not security sensitive
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

// dropLocked reports whether a data point with these attributes belongs to a dropped flow.
func (f *liveFlowsFilter) dropLocked(snapshot *liveFlowsSnapshot, now time.Time, attrs attribute.Set) bool {
	value, ok := attrs.Value(FlowNameKey)
	if !ok {
		return false
	}
	flowName := value.AsString()
	if _, live := snapshot.names[flowName]; live {
		return false
	}
	since, seen := f.mu.missingSince[flowName]
	if !seen {
		// We will start dropping this series after the next snapshot.
		f.mu.missingSince[flowName] = now
		return false
	}
	return since.Before(snapshot.takenAt)
}

// filter returns rm without data points of dropped flows. rm is not modified, since the SDK reuses it.
func (f *liveFlowsFilter) filter(rm *metricdata.ResourceMetrics) *metricdata.ResourceMetrics {
	snapshot := f.snapshot.Load()
	if snapshot == nil {
		return rm
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	now := f.now()
	drop := func(attrs attribute.Set) bool { return f.dropLocked(snapshot, now, attrs) }

	filtered := &metricdata.ResourceMetrics{
		Resource:     rm.Resource,
		ScopeMetrics: make([]metricdata.ScopeMetrics, 0, len(rm.ScopeMetrics)),
	}
	for _, scope := range rm.ScopeMetrics {
		metrics := make([]metricdata.Metrics, 0, len(scope.Metrics))
		for _, m := range scope.Metrics {
			if data, emptied := filterAggregation(m.Data, drop); !emptied {
				m.Data = data
				metrics = append(metrics, m)
			}
		}
		if len(metrics) > 0 {
			filtered.ScopeMetrics = append(filtered.ScopeMetrics, metricdata.ScopeMetrics{Scope: scope.Scope, Metrics: metrics})
		}
	}
	return filtered
}

// filterAggregation returns data without dropped points, and whether filtering removed every point.
func filterAggregation(data metricdata.Aggregation, drop func(attribute.Set) bool) (metricdata.Aggregation, bool) {
	var emptied bool
	switch d := data.(type) {
	case metricdata.Sum[int64]:
		d.DataPoints, emptied = filterPoints(d.DataPoints, drop, func(p *metricdata.DataPoint[int64]) attribute.Set { return p.Attributes })
		return d, emptied
	case metricdata.Sum[float64]:
		d.DataPoints, emptied = filterPoints(d.DataPoints, drop, func(p *metricdata.DataPoint[float64]) attribute.Set { return p.Attributes })
		return d, emptied
	case metricdata.Gauge[int64]:
		d.DataPoints, emptied = filterPoints(d.DataPoints, drop, func(p *metricdata.DataPoint[int64]) attribute.Set { return p.Attributes })
		return d, emptied
	case metricdata.Gauge[float64]:
		d.DataPoints, emptied = filterPoints(d.DataPoints, drop, func(p *metricdata.DataPoint[float64]) attribute.Set { return p.Attributes })
		return d, emptied
	case metricdata.Histogram[int64]:
		d.DataPoints, emptied = filterPoints(d.DataPoints, drop,
			func(p *metricdata.HistogramDataPoint[int64]) attribute.Set { return p.Attributes })
		return d, emptied
	case metricdata.Histogram[float64]:
		d.DataPoints, emptied = filterPoints(d.DataPoints, drop,
			func(p *metricdata.HistogramDataPoint[float64]) attribute.Set { return p.Attributes })
		return d, emptied
	case metricdata.ExponentialHistogram[int64]:
		d.DataPoints, emptied = filterPoints(d.DataPoints, drop,
			func(p *metricdata.ExponentialHistogramDataPoint[int64]) attribute.Set { return p.Attributes })
		return d, emptied
	case metricdata.ExponentialHistogram[float64]:
		d.DataPoints, emptied = filterPoints(d.DataPoints, drop,
			func(p *metricdata.ExponentialHistogramDataPoint[float64]) attribute.Set { return p.Attributes })
		return d, emptied
	case metricdata.Summary:
		d.DataPoints, emptied = filterPoints(d.DataPoints, drop, func(p *metricdata.SummaryDataPoint) attribute.Set { return p.Attributes })
		return d, emptied
	default:
		return data, false
	}
}

// filterPoints returns points without dropped ones, copying only if a point is dropped,
// and whether it dropped every point.
func filterPoints[P any](points []P, drop func(attribute.Set) bool, attrs func(*P) attribute.Set) ([]P, bool) {
	for i := range points {
		if !drop(attrs(&points[i])) {
			continue
		}
		kept := make([]P, i, len(points)-1)
		copy(kept, points[:i])
		for j := i + 1; j < len(points); j++ {
			if !drop(attrs(&points[j])) {
				kept = append(kept, points[j])
			}
		}
		return kept, len(kept) == 0
	}
	return points, false
}

type flowFilteringExporter struct {
	sdkmetric.Exporter
	filter *liveFlowsFilter
}

func (e *flowFilteringExporter) Export(ctx context.Context, rm *metricdata.ResourceMetrics) error {
	return e.Exporter.Export(ctx, e.filter.filter(rm))
}
