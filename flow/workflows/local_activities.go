package peerflow

import (
	"context"
	"fmt"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
)

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
