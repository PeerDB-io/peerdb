package otel_metrics

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peerdb/flow/shared"
)

const droppedFlowsEvictionInterval = 5 * time.Minute

// EvictDroppedFlows periodically removes the counter and gauge series of flows deleted from the catalog, until
// ctx is done. Each process lists flows itself, since a flow's series live in every process that ran its activities.
// Only instruments created before this call are covered, so the loop never reads the caches while they change.
func (om *OtelManager) EvictDroppedFlows(ctx context.Context, pool shared.CatalogPool) {
	if !om.Enabled {
		return
	}
	evicters := om.droppedFlowsEvicters()
	go func() {
		ticker := time.NewTicker(droppedFlowsEvictionInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			// Taken before the query: a series created after this may belong to a flow the query does not see yet.
			listedAt := time.Now()
			flowNames, err := listFlowNames(ctx, pool)
			if err != nil {
				if ctx.Err() == nil {
					slog.WarnContext(ctx, "failed to list flows to evict their metric series", slog.Any("error", err))
				}
				continue
			}
			evictDroppedFlows(evicters, flowNames, listedAt)
		}
	}()
}

func (om *OtelManager) droppedFlowsEvicters() []droppedFlowsEvicter {
	var evicters []droppedFlowsEvicter
	add := func(instrument any) {
		if evicter, ok := instrument.(droppedFlowsEvicter); ok {
			evicters = append(evicters, evicter)
		}
	}
	for _, counter := range om.Int64CountersCache {
		add(counter)
	}
	for _, gauge := range om.Int64GaugesCache {
		add(gauge)
	}
	for _, gauge := range om.Float64GaugesCache {
		add(gauge)
	}
	return evicters
}

func listFlowNames(ctx context.Context, pool shared.CatalogPool) ([]string, error) {
	rows, err := pool.Query(ctx, "SELECT name FROM flows")
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

func evictDroppedFlows(evicters []droppedFlowsEvicter, flowNames []string, listedAt time.Time) {
	live := make(map[string]struct{}, len(flowNames))
	for _, name := range flowNames {
		live[name] = struct{}{}
	}
	for _, evicter := range evicters {
		evicter.evictDroppedFlows(live, listedAt)
	}
}
