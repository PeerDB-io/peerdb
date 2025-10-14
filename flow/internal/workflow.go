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
			slog.WarnContext(ctx, "workflowId not found in catalog, will raise an error",
				slog.String("workflowId", workflowID))
			return protos.FlowStatus_STATUS_UNKNOWN, fmt.Errorf("workflowId not found in catalog: %w", err)
		}
		slog.ErrorContext(ctx, "failed to get status for flow from catalog",
			slog.Any("error", err),
			slog.String("flowID", workflowID))
		return flowStatus, fmt.Errorf("failed ot get status for flow from catalog: %w", err)
	} else if flowStatus == protos.FlowStatus_STATUS_UNKNOWN {
		slog.WarnContext(ctx, "flow status from catalog is unknown", slog.String("flowID", workflowID))
	}
	return flowStatus, nil
}

func UpdateFlowStatusInCatalog(ctx context.Context, pool shared.CatalogPool,
	workflowID string, status protos.FlowStatus,
) (protos.FlowStatus, error) {
	if _, err := pool.Exec(ctx, "UPDATE flows SET status=$1,updated_at=now() WHERE workflow_id=$2", status, workflowID); err != nil {
		slog.ErrorContext(ctx, "failed to update flow status", slog.Any("error", err), slog.String("flowID", workflowID))
		return status, fmt.Errorf("failed to update flow status: %w", err)
	}
	return status, nil
}
