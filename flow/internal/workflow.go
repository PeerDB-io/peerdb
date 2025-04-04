package internal

import (
	"context"
	"fmt"
	"log/slog"

	"go.temporal.io/sdk/client"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func getWorkflowStatusFromTemporal(ctx context.Context, temporalClient client.Client, workflowID string) (protos.FlowStatus, error) {
	res, err := temporalClient.QueryWorkflow(ctx, workflowID, "", shared.FlowStatusQuery)
	if err != nil {
		slog.Error("failed to query status in workflow with ID "+workflowID, slog.Any("error", err))
		return protos.FlowStatus_STATUS_UNKNOWN, fmt.Errorf("failed to query status in workflow with ID %s: %w", workflowID, err)
	}
	var state protos.FlowStatus
	if err := res.Get(&state); err != nil {
		slog.Error("failed to get status in workflow with ID "+workflowID, slog.Any("error", err))
		return protos.FlowStatus_STATUS_UNKNOWN, fmt.Errorf("failed to get status in workflow with ID %s: %w", workflowID, err)
	}
	return state, nil
}

func GetWorkflowStatus(ctx context.Context, pool shared.CatalogPool, temporalClient client.Client, workflowID string) (protos.FlowStatus, error) {
	var flowStatus protos.FlowStatus
	err := pool.QueryRow(ctx, "SELECT status FROM flows WHERE workflow_id = $1", workflowID).Scan(&flowStatus)
	if err != nil || flowStatus == protos.FlowStatus_STATUS_UNKNOWN {
		if err != nil {
			slog.Error("failed to get status for flow from catalog, will fall back to temporal",
				slog.Any("error", err),
				slog.String("flowID", workflowID))
		} else if flowStatus == protos.FlowStatus_STATUS_UNKNOWN {
			slog.Info("flow status from catalog is unknown, will fall back to temporal",
				slog.String("flowID", workflowID))
		}

		// this should only trigger for existing mirrors once
		// except QRep/XMin mirrors, they will always hit this codepath
		status, tctlErr := getWorkflowStatusFromTemporal(ctx, temporalClient, workflowID)
		if tctlErr != nil {
			return status, tctlErr
		}
		return UpdateFlowStatusInCatalog(ctx, pool, workflowID, status)
	}

	return flowStatus, nil
}

func UpdateFlowStatusInCatalog(ctx context.Context, pool shared.CatalogPool,
	workflowID string, status protos.FlowStatus,
) (protos.FlowStatus, error) {
	_, err := pool.Exec(ctx, "UPDATE flows SET status=$1,updated_at=now() WHERE workflow_id=$2", status, workflowID)
	if err != nil {
		slog.Error("failed to update flow status", slog.Any("error", err), slog.String("flowID", workflowID))
		return status, fmt.Errorf("failed to update flow status: %w", err)
	}
	return status, nil
}
