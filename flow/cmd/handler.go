package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peer-flow/alerting"
	"github.com/PeerDB-io/peer-flow/connectors"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/shared"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
)

// grpc server implementation
type FlowRequestHandler struct {
	protos.UnimplementedFlowServiceServer
	temporalClient      client.Client
	pool                *pgxpool.Pool
	alerter             *alerting.Alerter
	peerflowTaskQueueID string
}

func NewFlowRequestHandler(temporalClient client.Client, pool *pgxpool.Pool, taskQueue string) *FlowRequestHandler {
	return &FlowRequestHandler{
		temporalClient:      temporalClient,
		pool:                pool,
		peerflowTaskQueueID: taskQueue,
		alerter:             alerting.NewAlerter(context.Background(), pool),
	}
}

func (h *FlowRequestHandler) getPeerID(ctx context.Context, peerName string) (int32, int32, error) {
	var id pgtype.Int4
	var peerType pgtype.Int4
	err := h.pool.QueryRow(ctx, "SELECT id,type FROM peers WHERE name = $1", peerName).Scan(&id, &peerType)
	if err != nil {
		slog.Error("unable to query peer id for peer "+peerName, slog.Any("error", err))
		return -1, -1, fmt.Errorf("unable to query peer id for peer %s: %s", peerName, err)
	}
	return id.Int32, peerType.Int32, nil
}

func schemaForTableIdentifier(tableIdentifier string, peerDBType int32) string {
	if peerDBType != int32(protos.DBType_BIGQUERY) && !strings.ContainsRune(tableIdentifier, '.') {
		return "public." + tableIdentifier
	}
	return tableIdentifier
}

func (h *FlowRequestHandler) createCdcJobEntry(ctx context.Context,
	req *protos.CreateCDCFlowRequest, workflowID string,
) error {
	sourcePeerID, sourePeerType, srcErr := h.getPeerID(ctx, req.ConnectionConfigs.SourceName)
	if srcErr != nil {
		return fmt.Errorf("unable to get peer id for source peer %s: %w",
			req.ConnectionConfigs.SourceName, srcErr)
	}

	destinationPeerID, destinationPeerType, dstErr := h.getPeerID(ctx, req.ConnectionConfigs.DestinationName)
	if dstErr != nil {
		return fmt.Errorf("unable to get peer id for target peer %s: %w",
			req.ConnectionConfigs.DestinationName, srcErr)
	}

	for _, v := range req.ConnectionConfigs.TableMappings {
		_, err := h.pool.Exec(ctx, `
		INSERT INTO flows (workflow_id, name, source_peer, destination_peer, description,
		source_table_identifier, destination_table_identifier) VALUES ($1, $2, $3, $4, $5, $6, $7)
		`, workflowID, req.ConnectionConfigs.FlowJobName, sourcePeerID, destinationPeerID,
			"Mirror created via GRPC",
			schemaForTableIdentifier(v.SourceTableIdentifier, sourePeerType),
			schemaForTableIdentifier(v.DestinationTableIdentifier, destinationPeerType))
		if err != nil {
			return fmt.Errorf("unable to insert into flows table for flow %s with source table %s: %w",
				req.ConnectionConfigs.FlowJobName, v.SourceTableIdentifier, err)
		}
	}

	return nil
}

func (h *FlowRequestHandler) createQRepJobEntry(ctx context.Context,
	req *protos.CreateQRepFlowRequest, workflowID string,
) error {
	sourcePeerName := req.QrepConfig.SourceName
	sourcePeerID, _, srcErr := h.getPeerID(ctx, sourcePeerName)
	if srcErr != nil {
		return fmt.Errorf("unable to get peer id for source peer %s: %w",
			sourcePeerName, srcErr)
	}

	destinationPeerName := req.QrepConfig.DestinationName
	destinationPeerID, _, dstErr := h.getPeerID(ctx, destinationPeerName)
	if dstErr != nil {
		return fmt.Errorf("unable to get peer id for target peer %s: %w",
			destinationPeerName, srcErr)
	}
	flowName := req.QrepConfig.FlowJobName
	_, err := h.pool.Exec(ctx, `INSERT INTO flows(workflow_id,name, source_peer, destination_peer, description,
		destination_table_identifier, query_string) VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, workflowID, flowName, sourcePeerID, destinationPeerID,
		"Mirror created via GRPC",
		req.QrepConfig.DestinationTableIdentifier,
		req.QrepConfig.Query,
	)
	if err != nil {
		return fmt.Errorf("unable to insert into flows table for flow %s with source table %s: %w",
			flowName, req.QrepConfig.WatermarkTable, err)
	}

	return nil
}

func (h *FlowRequestHandler) CreateCDCFlow(
	ctx context.Context, req *protos.CreateCDCFlowRequest,
) (*protos.CreateCDCFlowResponse, error) {
	cfg := req.ConnectionConfigs
	_, validateErr := h.ValidateCDCMirror(ctx, req)
	if validateErr != nil {
		slog.Error("validate mirror error", slog.Any("error", validateErr))
		return nil, fmt.Errorf("invalid mirror: %w", validateErr)
	}

	workflowID := fmt.Sprintf("%s-peerflow-%s", cfg.FlowJobName, uuid.New())
	workflowOptions := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: h.peerflowTaskQueueID,
		SearchAttributes: map[string]interface{}{
			shared.MirrorNameSearchAttribute: cfg.FlowJobName,
		},
	}

	err := h.createCdcJobEntry(ctx, req, workflowID)
	if err != nil {
		slog.Error("unable to create flow job entry", slog.Any("error", err))
		return nil, fmt.Errorf("unable to create flow job entry: %w", err)
	}

	err = h.updateFlowConfigInCatalog(ctx, cfg)
	if err != nil {
		slog.Error("unable to update flow config in catalog", slog.Any("error", err))
		return nil, fmt.Errorf("unable to update flow config in catalog: %w", err)
	}

	_, err = h.temporalClient.ExecuteWorkflow(ctx, workflowOptions, peerflow.CDCFlowWorkflow, cfg, nil)
	if err != nil {
		slog.Error("unable to start PeerFlow workflow", slog.Any("error", err))
		return nil, fmt.Errorf("unable to start PeerFlow workflow: %w", err)
	}

	return &protos.CreateCDCFlowResponse{
		WorkflowId: workflowID,
	}, nil
}

func (h *FlowRequestHandler) updateFlowConfigInCatalog(
	ctx context.Context,
	cfg *protos.FlowConnectionConfigs,
) error {
	return shared.UpdateCDCConfigInCatalog(ctx, h.pool, slog.Default(), cfg)
}

func (h *FlowRequestHandler) removeFlowEntryInCatalog(
	ctx context.Context,
	flowName string,
) error {
	_, err := h.pool.Exec(ctx, "DELETE FROM flows WHERE name=$1", flowName)
	if err != nil {
		return fmt.Errorf("unable to remove flow entry in catalog: %w", err)
	}

	return nil
}

func (h *FlowRequestHandler) CreateQRepFlow(
	ctx context.Context, req *protos.CreateQRepFlowRequest,
) (*protos.CreateQRepFlowResponse, error) {
	cfg := req.QrepConfig
	workflowID := fmt.Sprintf("%s-qrepflow-%s", cfg.FlowJobName, uuid.New())
	workflowOptions := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: h.peerflowTaskQueueID,
		SearchAttributes: map[string]interface{}{
			shared.MirrorNameSearchAttribute: cfg.FlowJobName,
		},
	}
	if req.CreateCatalogEntry {
		if err := h.createQRepJobEntry(ctx, req, workflowID); err != nil {
			slog.Error("unable to create flow job entry",
				slog.Any("error", err), slog.String("flowName", cfg.FlowJobName))
			return nil, fmt.Errorf("unable to create flow job entry: %w", err)
		}
	}
	dbtype, err := connectors.LoadPeerType(ctx, h.pool, cfg.SourceName)
	if err != nil {
		return nil, err
	}
	var workflowFn interface{}
	if dbtype == protos.DBType_POSTGRES && cfg.WatermarkColumn == "xmin" {
		workflowFn = peerflow.XminFlowWorkflow
	} else {
		workflowFn = peerflow.QRepFlowWorkflow
	}

	if req.QrepConfig.SyncedAtColName == "" {
		cfg.SyncedAtColName = "_PEERDB_SYNCED_AT"
	}

	cfg.ParentMirrorName = cfg.FlowJobName

	if _, err := h.temporalClient.ExecuteWorkflow(ctx, workflowOptions, workflowFn, cfg, nil); err != nil {
		slog.Error("unable to start QRepFlow workflow",
			slog.Any("error", err), slog.String("flowName", cfg.FlowJobName))
		return nil, fmt.Errorf("unable to start QRepFlow workflow: %w", err)
	}

	err = h.updateQRepConfigInCatalog(ctx, cfg)
	if err != nil {
		slog.Error("unable to update qrep config in catalog",
			slog.Any("error", err), slog.String("flowName", cfg.FlowJobName))
		return nil, fmt.Errorf("unable to update qrep config in catalog: %w", err)
	}

	return &protos.CreateQRepFlowResponse{
		WorkflowId: workflowID,
	}, nil
}

func (h *FlowRequestHandler) updateQRepConfigInCatalog(
	ctx context.Context,
	cfg *protos.QRepConfig,
) error {
	var cfgBytes []byte
	var err error

	cfgBytes, err = proto.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("unable to marshal qrep config: %w", err)
	}

	_, err = h.pool.Exec(ctx,
		"UPDATE flows SET config_proto = $1 WHERE name = $2",
		cfgBytes, cfg.FlowJobName)
	if err != nil {
		return fmt.Errorf("unable to update qrep config in catalog: %w", err)
	}

	return nil
}

func (h *FlowRequestHandler) shutdownFlow(
	ctx context.Context,
	flowJobName string,
	deleteStats bool,
) error {
	workflowID, err := h.getWorkflowID(ctx, flowJobName)
	if err != nil {
		return err
	}

	logs := slog.Group("shutdown-log",
		slog.String(string(shared.FlowNameKey), flowJobName),
		slog.String("workflowId", workflowID),
	)

	err = h.handleCancelWorkflow(ctx, workflowID, "")
	if err != nil {
		slog.Error("unable to cancel workflow", logs, slog.Any("error", err))
		return fmt.Errorf("unable to wait for PeerFlow workflow to close: %w", err)
	}

	isCdc, err := h.isCDCFlow(ctx, flowJobName)
	if err != nil {
		slog.Error("unable to check if workflow is cdc", logs, slog.Any("error", err))
		return fmt.Errorf("unable to determine if workflow is cdc: %w", err)
	} else if isCdc {
		cdcConfig, err := h.getFlowConfigFromCatalog(ctx, flowJobName)
		if err != nil {
			slog.Error("unable to get cdc config from catalog", logs, slog.Any("error", err))
			return fmt.Errorf("unable to get cdc config from catalog: %w", err)
		}
		workflowID := fmt.Sprintf("%s-dropflow-%s", flowJobName, uuid.New())
		workflowOptions := client.StartWorkflowOptions{
			ID:        workflowID,
			TaskQueue: h.peerflowTaskQueueID,
			SearchAttributes: map[string]interface{}{
				shared.MirrorNameSearchAttribute: flowJobName,
			},
		}

		dropFlowHandle, err := h.temporalClient.ExecuteWorkflow(ctx, workflowOptions,
			peerflow.DropFlowWorkflow, &protos.DropFlowInput{
				FlowJobName:         flowJobName,
				SourcePeerName:      cdcConfig.SourceName,
				DestinationPeerName: cdcConfig.DestinationName,
				DropFlowStats:       deleteStats,
			})
		if err != nil {
			slog.Error("unable to start DropFlow workflow",
				logs,
				slog.Any("error", err))
			return fmt.Errorf("unable to start DropFlow workflow: %w", err)
		}

		cancelCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()

		errChan := make(chan error, 1)
		go func() {
			errChan <- dropFlowHandle.Get(cancelCtx, nil)
		}()

		select {
		case err := <-errChan:
			if err != nil {
				slog.Error("DropFlow workflow did not execute successfully",
					logs,
					slog.Any("error", err),
				)
				return fmt.Errorf("DropFlow workflow did not execute successfully: %w", err)
			}
		case <-time.After(5 * time.Minute):
			err := h.handleCancelWorkflow(ctx, workflowID, "")
			if err != nil {
				slog.Error("unable to wait for DropFlow workflow to close", logs, slog.Any("error", err))
				return fmt.Errorf("unable to wait for DropFlow workflow to close: %w", err)
			}
		}
	}

	err = h.removeFlowEntryInCatalog(ctx, flowJobName)
	if err != nil {
		slog.Error("unable to remove flow job entry",
			slog.String(string(shared.FlowNameKey), flowJobName),
			slog.Any("error", err),
			slog.String("workflowId", workflowID))
		return err
	}

	return nil
}

func (h *FlowRequestHandler) FlowStateChange(
	ctx context.Context,
	req *protos.FlowStateChangeRequest,
) (*protos.FlowStateChangeResponse, error) {
	slog.Info("FlowStateChange called", slog.String("flowJobName", req.FlowJobName), slog.Any("req", req))
	workflowID, err := h.getWorkflowID(ctx, req.FlowJobName)
	if err != nil {
		slog.Error("[flow-state-change]unable to get workflowID", slog.Any("error", err))
		return nil, err
	}
	currState, err := h.getWorkflowStatus(ctx, workflowID)
	if err != nil {
		slog.Error("[flow-state-change]unable to get workflow status", slog.Any("error", err))
		return nil, err
	}

	if req.FlowConfigUpdate != nil && req.FlowConfigUpdate.GetCdcFlowConfigUpdate() != nil {
		err = model.CDCDynamicPropertiesSignal.SignalClientWorkflow(
			ctx,
			h.temporalClient,
			workflowID,
			"",
			req.FlowConfigUpdate.GetCdcFlowConfigUpdate(),
		)
		if err != nil {
			slog.Error("unable to signal workflow", slog.Any("error", err))
			return nil, fmt.Errorf("unable to signal workflow: %w", err)
		}
	}

	if req.RequestedFlowState != protos.FlowStatus_STATUS_UNKNOWN {
		if req.RequestedFlowState == protos.FlowStatus_STATUS_PAUSED &&
			currState == protos.FlowStatus_STATUS_RUNNING {
			slog.Info("[flow-state-change]: received pause request")
			err = model.FlowSignal.SignalClientWorkflow(
				ctx,
				h.temporalClient,
				workflowID,
				"",
				model.PauseSignal,
			)
		} else if req.RequestedFlowState == protos.FlowStatus_STATUS_RUNNING &&
			currState == protos.FlowStatus_STATUS_PAUSED {
			slog.Info("[flow-state-change]: received resume request")
			err = model.FlowSignal.SignalClientWorkflow(
				ctx,
				h.temporalClient,
				workflowID,
				"",
				model.NoopSignal,
			)
		} else if req.RequestedFlowState == protos.FlowStatus_STATUS_TERMINATED &&
			(currState != protos.FlowStatus_STATUS_TERMINATED) {
			slog.Info("[flow-state-change]: received drop mirror request")
			err = h.shutdownFlow(ctx, req.FlowJobName, req.DropMirrorStats)
		} else if req.RequestedFlowState != currState {
			slog.Error("illegal state change requested", slog.Any("requestedFlowState", req.RequestedFlowState),
				slog.Any("currState", currState))
			return nil, fmt.Errorf("illegal state change requested: %v, current state is: %v",
				req.RequestedFlowState, currState)
		}
		if err != nil {
			slog.Error("unable to signal workflow", slog.Any("error", err))
			return nil, fmt.Errorf("unable to signal workflow: %w", err)
		}
	}

	return &protos.FlowStateChangeResponse{
		Ok: true,
	}, nil
}

func (h *FlowRequestHandler) handleCancelWorkflow(ctx context.Context, workflowID, runID string) error {
	errChan := make(chan error, 1)

	// Create a new context with timeout for CancelWorkflow
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	// Call CancelWorkflow in a goroutine
	go func() {
		err := h.temporalClient.CancelWorkflow(ctxWithTimeout, workflowID, runID)
		errChan <- err
	}()

	select {
	case err := <-errChan:
		if err != nil {
			slog.Error(fmt.Sprintf("unable to cancel PeerFlow workflow: %s. Attempting to terminate.", err.Error()))
			terminationReason := fmt.Sprintf("workflow %s did not cancel in time.", workflowID)
			if err = h.temporalClient.TerminateWorkflow(ctx, workflowID, runID, terminationReason); err != nil {
				return fmt.Errorf("unable to terminate PeerFlow workflow: %w", err)
			}
		}
	case <-time.After(1 * time.Minute):
		// If 1 minute has passed and we haven't received an error, terminate the workflow
		slog.Error("Timeout reached while trying to cancel PeerFlow workflow. Attempting to terminate.")
		terminationReason := fmt.Sprintf("workflow %s did not cancel in time.", workflowID)
		if err := h.temporalClient.TerminateWorkflow(ctx, workflowID, runID, terminationReason); err != nil {
			return fmt.Errorf("unable to terminate PeerFlow workflow: %w", err)
		}
	}

	return nil
}

func (h *FlowRequestHandler) CreatePeer(
	ctx context.Context,
	req *protos.CreatePeerRequest,
) (*protos.CreatePeerResponse, error) {
	status, validateErr := h.ValidatePeer(ctx, &protos.ValidatePeerRequest{Peer: req.Peer})
	if validateErr != nil {
		return nil, validateErr
	}
	if status.Status != protos.ValidatePeerStatus_VALID {
		return &protos.CreatePeerResponse{
			Status:  protos.CreatePeerStatus_FAILED,
			Message: status.Message,
		}, nil
	}

	return utils.CreatePeerNoValidate(ctx, h.pool, req.Peer, req.AllowUpdate)
}

func (h *FlowRequestHandler) DropPeer(
	ctx context.Context,
	req *protos.DropPeerRequest,
) (*protos.DropPeerResponse, error) {
	if req.PeerName == "" {
		return &protos.DropPeerResponse{
			Ok:           false,
			ErrorMessage: fmt.Sprintf("Peer %s not found", req.PeerName),
		}, fmt.Errorf("peer %s not found", req.PeerName)
	}

	// Check if peer name is in flows table
	peerID, _, err := h.getPeerID(ctx, req.PeerName)
	if err != nil {
		return &protos.DropPeerResponse{
			Ok:           false,
			ErrorMessage: fmt.Sprintf("Failed to obtain peer ID for peer %s: %v", req.PeerName, err),
		}, fmt.Errorf("failed to obtain peer ID for peer %s: %v", req.PeerName, err)
	}

	var inMirror pgtype.Int8
	queryErr := h.pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM flows WHERE source_peer=$1 or destination_peer=$2",
		peerID, peerID).Scan(&inMirror)
	if queryErr != nil {
		return &protos.DropPeerResponse{
			Ok:           false,
			ErrorMessage: fmt.Sprintf("Failed to check for existing mirrors with peer %s: %v", req.PeerName, queryErr),
		}, fmt.Errorf("failed to check for existing mirrors with peer %s", req.PeerName)
	}

	if inMirror.Int64 != 0 {
		return &protos.DropPeerResponse{
			Ok:           false,
			ErrorMessage: fmt.Sprintf("Peer %s is currently involved in an ongoing mirror.", req.PeerName),
		}, nil
	}

	_, delErr := h.pool.Exec(ctx, "DELETE FROM peers WHERE name = $1", req.PeerName)
	if delErr != nil {
		return &protos.DropPeerResponse{
			Ok:           false,
			ErrorMessage: fmt.Sprintf("failed to delete peer %s from metadata table: %v", req.PeerName, delErr),
		}, fmt.Errorf("failed to delete peer %s from metadata table: %v", req.PeerName, delErr)
	}

	return &protos.DropPeerResponse{
		Ok: true,
	}, nil
}

func (h *FlowRequestHandler) getWorkflowID(ctx context.Context, flowJobName string) (string, error) {
	q := "SELECT workflow_id FROM flows WHERE name = $1"
	row := h.pool.QueryRow(ctx, q, flowJobName)
	var workflowID string
	if err := row.Scan(&workflowID); err != nil {
		return "", fmt.Errorf("unable to get workflowID for flow job %s: %w", flowJobName, err)
	}

	return workflowID, nil
}

// only supports CDC resync for now
func (h *FlowRequestHandler) ResyncMirror(
	ctx context.Context,
	req *protos.ResyncMirrorRequest,
) (*protos.ResyncMirrorResponse, error) {
	isCDC, err := h.isCDCFlow(ctx, req.FlowJobName)
	if err != nil {
		return nil, err
	}
	if !isCDC {
		return nil, errors.New("resync is only supported for CDC mirrors")
	}
	// getting config before dropping the flow since the flow entry is deleted unconditionally
	config, err := h.getFlowConfigFromCatalog(ctx, req.FlowJobName)
	if err != nil {
		return nil, err
	}

	err = h.shutdownFlow(ctx, req.FlowJobName, req.DropStats)
	if err != nil {
		return nil, err
	}
	config.Resync = true
	config.DoInitialSnapshot = true

	_, err = h.CreateCDCFlow(ctx, &protos.CreateCDCFlowRequest{
		ConnectionConfigs: config,
	})
	if err != nil {
		return nil, err
	}
	return &protos.ResyncMirrorResponse{
		Ok: true,
	}, nil
}
