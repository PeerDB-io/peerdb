package internal

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func GetWorkflowStatus(ctx context.Context, pool shared.CatalogPool,
	workflowID string,
) (protos.FlowStatus, error) {
	var flowStatus protos.FlowStatus
	err := pool.QueryRow(ctx, "SELECT status FROM flows WHERE workflow_id = $1", workflowID).Scan(&flowStatus)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			slog.Warn("workflowId not found in catalog, will raise an error",
				slog.String("workflowId", workflowID))
			return protos.FlowStatus_STATUS_UNKNOWN, fmt.Errorf("workflowId not found in catalog: %w", err)
		}
		slog.Error("failed to get status for flow from catalog",
			slog.Any("error", err),
			slog.String("flowID", workflowID))
		return flowStatus, fmt.Errorf("failed ot get status for flow from catalog: %w", err)
	} else if flowStatus == protos.FlowStatus_STATUS_UNKNOWN {
		slog.Warn("flow status from catalog is unknown", slog.String("flowID", workflowID))
	}
	return flowStatus, nil
}
