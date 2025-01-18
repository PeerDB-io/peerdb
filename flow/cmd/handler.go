package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peerdb/flow/alerting"
	"github.com/PeerDB-io/peerdb/flow/connectors"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/peerdbenv"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/mirrorutils"
	peerflow "github.com/PeerDB-io/peerdb/flow/workflows"
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

func (h *FlowRequestHandler) createQRepJobEntry(ctx context.Context,
	req *protos.CreateQRepFlowRequest, workflowID string,
) error {
	sourcePeerName := req.QrepConfig.SourceName
	sourcePeerID, _, srcErr := mirrorutils.GetPeerID(ctx, h.pool, sourcePeerName)
	if srcErr != nil {
		return fmt.Errorf("unable to get peer id for source peer %s: %w",
			sourcePeerName, srcErr)
	}

	destinationPeerName := req.QrepConfig.DestinationName
	destinationPeerID, _, dstErr := mirrorutils.GetPeerID(ctx, h.pool, destinationPeerName)
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
	underMaintenance, err := peerdbenv.PeerDBMaintenanceModeEnabled(ctx, nil)
	if err != nil {
		slog.Error("unable to check maintenance mode", slog.Any("error", err))
		return nil, fmt.Errorf("unable to load dynamic config: %w", err)
	}
	if underMaintenance {
		slog.Warn("Validate request denied due to maintenance", "flowName", req.ConnectionConfigs.FlowJobName)
		return nil, errors.New("PeerDB is under maintenance")
	}

	cfg := req.ConnectionConfigs

	// For resync, we validate the mirror before dropping it and getting to this step.
	// There is no point validating again here if it's a resync - the mirror is dropped already
	if !cfg.Resync {
		_, validateErr := h.ValidateCDCMirror(ctx, req)
		if validateErr != nil {
			slog.Error("validate mirror error", slog.Any("error", validateErr))
			return nil, fmt.Errorf("invalid mirror: %w", validateErr)
		}
	}

	workflowID := fmt.Sprintf("%s-peerflow-%s", cfg.FlowJobName, uuid.New())
	workflowOptions := client.StartWorkflowOptions{
		ID:                    workflowID,
		TaskQueue:             h.peerflowTaskQueueID,
		TypedSearchAttributes: shared.NewSearchAttributes(cfg.FlowJobName),
	}

	if err := mirrorutils.CreateCDCWorkflowEntry(ctx, h.pool, req.ConnectionConfigs, workflowID); err != nil {
		slog.Error("unable to create flow job entry", slog.Any("error", err))
		return nil, fmt.Errorf("unable to create flow job entry: %w", err)
	}

	if err := shared.UpdateCDCConfigInCatalog(ctx, h.pool, slog.Default(), cfg); err != nil {
		slog.Error("unable to update flow config in catalog", slog.Any("error", err))
		return nil, fmt.Errorf("unable to update flow config in catalog: %w", err)
	}

	if _, err := h.temporalClient.ExecuteWorkflow(ctx, workflowOptions, peerflow.CDCFlowWorkflow, cfg, nil); err != nil {
		slog.Error("unable to start PeerFlow workflow", slog.Any("error", err))
		return nil, fmt.Errorf("unable to start PeerFlow workflow: %w", err)
	}

	return &protos.CreateCDCFlowResponse{
		WorkflowId: workflowID,
	}, nil
}

func (h *FlowRequestHandler) CreateQRepFlow(
	ctx context.Context, req *protos.CreateQRepFlowRequest,
) (*protos.CreateQRepFlowResponse, error) {
	cfg := req.QrepConfig
	workflowID := fmt.Sprintf("%s-qrepflow-%s", cfg.FlowJobName, uuid.New())
	workflowOptions := client.StartWorkflowOptions{
		ID:                    workflowID,
		TaskQueue:             h.peerflowTaskQueueID,
		TypedSearchAttributes: shared.NewSearchAttributes(cfg.FlowJobName),
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
	cfgBytes, err := proto.Marshal(cfg)
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

	isCdc, err := h.isCDCFlow(ctx, flowJobName)
	if err != nil {
		slog.Error("unable to check if workflow is cdc", logs, slog.Any("error", err))
		return fmt.Errorf("unable to determine if workflow is cdc: %w", err)
	}
	var cdcConfig *protos.FlowConnectionConfigs
	if isCdc {
		cdcConfig, err = h.getFlowConfigFromCatalog(ctx, flowJobName)
		if err != nil {
			slog.Error("unable to get cdc config from catalog", logs, slog.Any("error", err))
			return fmt.Errorf("unable to get cdc config from catalog: %w", err)
		}
	}
	dropFlowWorkflowID := fmt.Sprintf("%s-dropflow-%s", flowJobName, uuid.New())
	workflowOptions := client.StartWorkflowOptions{
		ID:                    dropFlowWorkflowID,
		TaskQueue:             h.peerflowTaskQueueID,
		TypedSearchAttributes: shared.NewSearchAttributes(flowJobName),
	}

	if _, err := h.temporalClient.ExecuteWorkflow(ctx, workflowOptions,
		peerflow.DropFlowWorkflow, &protos.DropFlowInput{
			FlowJobName:           flowJobName,
			DropFlowStats:         deleteStats,
			FlowConnectionConfigs: cdcConfig,
			WorkflowId:            workflowID,
			CdcResync:             false,
		}); err != nil {
		slog.Error("unable to start DropFlow workflow", logs, slog.Any("error", err))
		return fmt.Errorf("unable to start DropFlow workflow: %w", err)
	}

	return nil
}

func (h *FlowRequestHandler) FlowStateChange(
	ctx context.Context,
	req *protos.FlowStateChangeRequest,
) (*protos.FlowStateChangeResponse, error) {
	logs := slog.String("flowJobName", req.FlowJobName)
	slog.Info("FlowStateChange called", logs, slog.Any("req", req))
	underMaintenance, err := peerdbenv.PeerDBMaintenanceModeEnabled(ctx, nil)
	if err != nil {
		slog.Error("unable to check maintenance mode", logs, slog.Any("error", err))
		return nil, fmt.Errorf("unable to load dynamic config: %w", err)
	}

	if underMaintenance {
		slog.Warn("Flow state change request denied due to maintenance", logs)
		return nil, errors.New("PeerDB is under maintenance")
	}

	workflowID, err := h.getWorkflowID(ctx, req.FlowJobName)
	if err != nil {
		slog.Error("[flow-state-change] unable to get workflowID", logs, slog.Any("error", err))
		return nil, err
	}
	currState, err := h.getWorkflowStatus(ctx, workflowID)
	if err != nil {
		slog.Error("[flow-state-change] unable to get workflow status", logs, slog.Any("error", err))
		return nil, err
	}

	if req.FlowConfigUpdate != nil && req.FlowConfigUpdate.GetCdcFlowConfigUpdate() != nil {
		err := model.CDCDynamicPropertiesSignal.SignalClientWorkflow(
			ctx,
			h.temporalClient,
			workflowID,
			"",
			req.FlowConfigUpdate.GetCdcFlowConfigUpdate(),
		)
		if err != nil {
			slog.Error("unable to signal workflow", logs, slog.Any("error", err))
			return nil, fmt.Errorf("unable to signal workflow: %w", err)
		}
	}

	if req.RequestedFlowState != protos.FlowStatus_STATUS_UNKNOWN {
		if req.RequestedFlowState == protos.FlowStatus_STATUS_PAUSED &&
			currState == protos.FlowStatus_STATUS_RUNNING {
			slog.Info("[flow-state-change] received pause request", logs)
			err = model.FlowSignal.SignalClientWorkflow(
				ctx,
				h.temporalClient,
				workflowID,
				"",
				model.PauseSignal,
			)
		} else if req.RequestedFlowState == protos.FlowStatus_STATUS_RUNNING &&
			currState == protos.FlowStatus_STATUS_PAUSED {
			slog.Info("[flow-state-change] received resume request", logs)
			err = model.FlowSignal.SignalClientWorkflow(
				ctx,
				h.temporalClient,
				workflowID,
				"",
				model.NoopSignal,
			)
		} else if req.RequestedFlowState == protos.FlowStatus_STATUS_TERMINATED &&
			(currState != protos.FlowStatus_STATUS_TERMINATED) {
			slog.Info("[flow-state-change] received drop mirror request", logs)
			err = h.shutdownFlow(ctx, req.FlowJobName, req.DropMirrorStats)
		} else if req.RequestedFlowState != currState {
			slog.Error("illegal state change requested", logs, slog.Any("requestedFlowState", req.RequestedFlowState),
				slog.Any("currState", currState))
			return nil, fmt.Errorf("illegal state change requested: %v, current state is: %v",
				req.RequestedFlowState, currState)
		}
		if err != nil {
			slog.Error("unable to signal workflow", logs, slog.Any("error", err))
			return nil, fmt.Errorf("unable to signal workflow: %w", err)
		}
	}

	return &protos.FlowStateChangeResponse{}, nil
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
		return nil, fmt.Errorf("peer %s not found", req.PeerName)
	}

	// Check if peer name is in flows table
	peerID, _, err := mirrorutils.GetPeerID(ctx, h.pool, req.PeerName)
	if err != nil {
		return nil, fmt.Errorf("failed to obtain peer ID for peer %s: %w", req.PeerName, err)
	}

	var inMirror pgtype.Int8
	if queryErr := h.pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM flows WHERE source_peer=$1 or destination_peer=$1", peerID,
	).Scan(&inMirror); queryErr != nil {
		return nil, fmt.Errorf("failed to check for existing mirrors with peer %s: %w", req.PeerName, queryErr)
	}

	if inMirror.Int64 != 0 {
		return nil, fmt.Errorf("peer %s is currently involved in an ongoing mirror", req.PeerName)
	}

	if _, delErr := h.pool.Exec(ctx, "DELETE FROM peers WHERE name = $1", req.PeerName); delErr != nil {
		return nil, fmt.Errorf("failed to delete peer %s from metadata table: %w", req.PeerName, delErr)
	}

	return &protos.DropPeerResponse{}, nil
}

func (h *FlowRequestHandler) getWorkflowID(ctx context.Context, flowJobName string) (string, error) {
	q := "SELECT workflow_id FROM flows WHERE name = $1"
	var workflowID string
	if err := h.pool.QueryRow(ctx, q, flowJobName).Scan(&workflowID); err != nil {
		return "", fmt.Errorf("unable to get workflowID for flow job %s: %w", flowJobName, err)
	}

	return workflowID, nil
}

// only supports CDC resync for now
func (h *FlowRequestHandler) ResyncMirror(
	ctx context.Context,
	req *protos.ResyncMirrorRequest,
) (*protos.ResyncMirrorResponse, error) {
	underMaintenance, err := peerdbenv.PeerDBMaintenanceModeEnabled(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to get maintenance mode status: %w", err)
	}
	if underMaintenance {
		return nil, errors.New("PeerDB is under maintenance")
	}

	isCDC, err := h.isCDCFlow(ctx, req.FlowJobName)
	if err != nil {
		return nil, err
	}
	if !isCDC {
		return nil, errors.New("resync is only supported for CDC mirrors")
	}

	workflowID, err := h.getWorkflowID(ctx, req.FlowJobName)
	if err != nil {
		return nil, err
	}

	// getting config before dropping the flow since the flow entry is deleted unconditionally
	config, err := h.getFlowConfigFromCatalog(ctx, req.FlowJobName)
	if err != nil {
		return nil, err
	}

	dropFlowWorkflowID := fmt.Sprintf("%s-dropflow-%s", req.FlowJobName, uuid.New())
	workflowOptions := client.StartWorkflowOptions{
		ID:                    dropFlowWorkflowID,
		TaskQueue:             h.peerflowTaskQueueID,
		TypedSearchAttributes: shared.NewSearchAttributes(req.FlowJobName),
	}

	if _, err := h.temporalClient.ExecuteWorkflow(ctx, workflowOptions,
		peerflow.DropFlowWorkflow, &protos.DropFlowInput{
			FlowJobName:           req.FlowJobName,
			DropFlowStats:         req.DropStats,
			FlowConnectionConfigs: config,
			WorkflowId:            workflowID,
			CdcResync:             true,
		}); err != nil {
		slog.Error("unable to start DropFlow workflow", slog.Any("error", err))
		return nil, fmt.Errorf("unable to start DropFlow workflow: %w", err)
	}

	return &protos.ResyncMirrorResponse{}, nil
}

func (h *FlowRequestHandler) GetInstanceInfo(ctx context.Context, in *protos.InstanceInfoRequest) (*protos.InstanceInfoResponse, error) {
	enabled, err := peerdbenv.PeerDBMaintenanceModeEnabled(ctx, nil)
	if err != nil {
		slog.Error("unable to get maintenance mode status", slog.Any("error", err))
		return &protos.InstanceInfoResponse{
			Status: protos.InstanceStatus_INSTANCE_STATUS_UNKNOWN,
		}, fmt.Errorf("unable to get maintenance mode status: %w", err)
	}
	if enabled {
		return &protos.InstanceInfoResponse{
			Status: protos.InstanceStatus_INSTANCE_STATUS_MAINTENANCE,
		}, nil
	}
	return &protos.InstanceInfoResponse{
		Status: protos.InstanceStatus_INSTANCE_STATUS_READY,
	}, nil
}

func (h *FlowRequestHandler) Maintenance(ctx context.Context, in *protos.MaintenanceRequest) (*protos.MaintenanceResponse, error) {
	taskQueueId := shared.MaintenanceFlowTaskQueue
	if in.UsePeerflowTaskQueue {
		taskQueueId = shared.PeerFlowTaskQueue
	}
	switch {
	case in.Status == protos.MaintenanceStatus_MAINTENANCE_STATUS_START:
		workflowRun, err := peerflow.RunStartMaintenanceWorkflow(ctx, h.temporalClient, &protos.StartMaintenanceFlowInput{}, taskQueueId)
		if err != nil {
			return nil, err
		}
		return &protos.MaintenanceResponse{
			WorkflowId: workflowRun.GetID(),
			RunId:      workflowRun.GetRunID(),
		}, nil
	case in.Status == protos.MaintenanceStatus_MAINTENANCE_STATUS_END:
		workflowRun, err := peerflow.RunEndMaintenanceWorkflow(ctx, h.temporalClient, &protos.EndMaintenanceFlowInput{}, taskQueueId)
		if err != nil {
			return nil, err
		}
		return &protos.MaintenanceResponse{
			WorkflowId: workflowRun.GetID(),
			RunId:      workflowRun.GetRunID(),
		}, nil
	}
	return nil, errors.New("invalid maintenance status")
}
