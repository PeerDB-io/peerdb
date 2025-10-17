package peerflow

import (
	"context"
	"fmt"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

// Do not rename this local activity, unless you know exactly what you are doing.
//
// Currently, when flow is signaled to pause during upgrades, we clear flow history
// in Temporal with Continue-As-New, leaving only `updateFlowStatusInCatalogActivity`
// in event history. Renaming this method will lead to Temporal failure to replay and
// cause workflow to panic [TMPRL1100].
func updateFlowStatusInCatalogActivity(
	ctx context.Context,
	workflowID string,
	status protos.FlowStatus,
) (protos.FlowStatus, error) {
	pool, err := internal.GetCatalogConnectionPoolFromEnv(ctx)
	if err != nil {
		return status, fmt.Errorf("failed to get catalog connection pool: %w", err)
	}
	return internal.UpdateFlowStatusInCatalog(ctx, pool, workflowID, status)
}

func updateFlowStatusWithNameInCatalogActivity(
	ctx context.Context,
	flowName string,
	status protos.FlowStatus,
) (protos.FlowStatus, error) {
	pool, err := internal.GetCatalogConnectionPoolFromEnv(ctx)
	if err != nil {
		return status, fmt.Errorf("failed to get catalog connection pool: %w", err)
	}
	return internal.UpdateFlowStatusWithNameInCatalog(ctx, pool, flowName, status)
}
