package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	tEnums "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/PeerDB-io/peerdb/flow/alerting"
	"github.com/PeerDB-io/peerdb/flow/connectors"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/shared/concurrency"
	"github.com/PeerDB-io/peerdb/flow/shared/exceptions"
	peerflow "github.com/PeerDB-io/peerdb/flow/workflows"
)

// grpc server implementation
type FlowRequestHandler struct {
	protos.UnimplementedFlowServiceServer
	temporalClient      client.Client
	pool                shared.CatalogPool
	alerter             *alerting.Alerter
	peerflowTaskQueueID string
}

func NewFlowRequestHandler(ctx context.Context, temporalClient client.Client, pool shared.CatalogPool, taskQueue string) *FlowRequestHandler {
	return &FlowRequestHandler{
		temporalClient:      temporalClient,
		pool:                pool,
		peerflowTaskQueueID: taskQueue,
		alerter:             alerting.NewAlerter(ctx, pool, nil),
	}
}

func (h *FlowRequestHandler) getPeerID(ctx context.Context, peerName string) (int32, error) {
	var id pgtype.Int4
	var peerType pgtype.Int4
	err := h.pool.QueryRow(ctx, "SELECT id,type FROM peers WHERE name = $1", peerName).Scan(&id, &peerType)
	if err != nil {
		slog.Error("unable to query peer id for peer "+peerName, slog.Any("error", err))
		return -1, fmt.Errorf("unable to query peer id for peer %s: %s", peerName, err)
	}
	return id.Int32, nil
}

func (h *FlowRequestHandler) createCdcJobEntry(ctx context.Context,
	req *protos.CreateCDCFlowRequest, workflowID string,
) error {
	sourcePeerID, srcErr := h.getPeerID(ctx, req.ConnectionConfigs.SourceName)
	if srcErr != nil {
		return fmt.Errorf("unable to get peer id for source peer %s: %w",
			req.ConnectionConfigs.SourceName, srcErr)
	}

	destinationPeerID, dstErr := h.getPeerID(ctx, req.ConnectionConfigs.DestinationName)
	if dstErr != nil {
		return fmt.Errorf("unable to get peer id for target peer %s: %w",
			req.ConnectionConfigs.DestinationName, dstErr)
	}

	cfgBytes, err := proto.Marshal(req.ConnectionConfigs)
	if err != nil {
		return fmt.Errorf("unable to marshal flow config: %w", err)
	}
	if _, err := h.pool.Exec(ctx,
		`INSERT INTO flows (workflow_id, name, source_peer, destination_peer, config_proto, status,
		description, source_table_identifier, destination_table_identifier) VALUES ($1,$2,$3,$4,$5,$6,'gRPC','','')`,
		workflowID, req.ConnectionConfigs.FlowJobName, sourcePeerID, destinationPeerID, cfgBytes, protos.FlowStatus_STATUS_SETUP,
	); err != nil {
		return fmt.Errorf("unable to insert into flows table for flow %s: %w",
			req.ConnectionConfigs.FlowJobName, err)
	}

	return nil
}

func (h *FlowRequestHandler) createQRepJobEntry(ctx context.Context,
	req *protos.CreateQRepFlowRequest, workflowID string,
) error {
	sourcePeerName := req.QrepConfig.SourceName
	sourcePeerID, srcErr := h.getPeerID(ctx, sourcePeerName)
	if srcErr != nil {
		return fmt.Errorf("unable to get peer id for source peer %s: %w",
			sourcePeerName, srcErr)
	}

	destinationPeerName := req.QrepConfig.DestinationName
	destinationPeerID, dstErr := h.getPeerID(ctx, destinationPeerName)
	if dstErr != nil {
		return fmt.Errorf("unable to get peer id for target peer %s: %w",
			destinationPeerName, dstErr)
	}

	cfgBytes, err := proto.Marshal(req.QrepConfig)
	if err != nil {
		return fmt.Errorf("unable to marshal qrep config: %w", err)
	}

	flowName := req.QrepConfig.FlowJobName
	if _, err := h.pool.Exec(ctx, `INSERT INTO flows(workflow_id,name,source_peer,destination_peer,config_proto,status,
		description, destination_table_identifier, query_string) VALUES ($1,$2,$3,$4,$5,$6,'gRPC',$7,$8)
	`, workflowID, flowName, sourcePeerID, destinationPeerID, cfgBytes, protos.FlowStatus_STATUS_RUNNING,
		req.QrepConfig.DestinationTableIdentifier,
		req.QrepConfig.Query,
	); err != nil {
		return fmt.Errorf("unable to insert into flows table for flow %s with source table %s: %w",
			flowName, req.QrepConfig.WatermarkTable, err)
	}

	return nil
}

func (h *FlowRequestHandler) CreateCDCFlow(
	ctx context.Context, req *protos.CreateCDCFlowRequest,
) (*protos.CreateCDCFlowResponse, error) {
	cfg := req.ConnectionConfigs
	internalVersion, err := internal.PeerDBForceInternalVersion(ctx, req.ConnectionConfigs.Env)
	if err != nil {
		return nil, err
	}
	cfg.Version = internalVersion

	// For resync, we validate the mirror before dropping it and getting to this step.
	// There is no point validating again here if it's a resync - the mirror is dropped already
	if !cfg.Resync {
		if _, err := h.ValidateCDCMirror(ctx, req); err != nil {
			slog.Error("validate mirror error", slog.Any("error", err))
			return nil, fmt.Errorf("invalid mirror: %w", err)
		}
	}

	workflowID := fmt.Sprintf("%s-peerflow-%s", cfg.FlowJobName, uuid.New())
	workflowOptions := client.StartWorkflowOptions{
		ID:                    workflowID,
		TaskQueue:             h.peerflowTaskQueueID,
		TypedSearchAttributes: shared.NewSearchAttributes(cfg.FlowJobName),
	}

	if err := h.createCdcJobEntry(ctx, req, workflowID); err != nil {
		slog.Error("unable to create flow job entry", slog.Any("error", err))
		return nil, fmt.Errorf("unable to create flow job entry: %w", err)
	}

	// clear the table mappings; we are pulling them from the DB.
	cfg.TableMappings = []*protos.TableMapping{}

	if _, err := h.temporalClient.ExecuteWorkflow(ctx, workflowOptions, peerflow.CDCFlowWorkflow, cfg.FlowJobName, nil); err != nil {
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
	internalVersion, err := internal.PeerDBForceInternalVersion(ctx, req.QrepConfig.Env)
	if err != nil {
		return nil, err
	}
	cfg.Version = internalVersion

	workflowID := fmt.Sprintf("%s-qrepflow-%s", cfg.FlowJobName, uuid.New())
	workflowOptions := client.StartWorkflowOptions{
		ID:                    workflowID,
		TaskQueue:             h.peerflowTaskQueueID,
		TypedSearchAttributes: shared.NewSearchAttributes(cfg.FlowJobName),
	}
	if err := h.createQRepJobEntry(ctx, req, workflowID); err != nil {
		slog.Error("unable to create flow job entry",
			slog.Any("error", err), slog.String("flowName", cfg.FlowJobName))
		return nil, fmt.Errorf("unable to create flow job entry: %w", err)
	}
	dbtype, err := connectors.LoadPeerType(ctx, h.pool, cfg.SourceName)
	if err != nil {
		return nil, err
	}
	var workflowFn any
	if dbtype == protos.DBType_POSTGRES && cfg.WatermarkColumn == "xmin" {
		workflowFn = peerflow.XminFlowWorkflow
	} else {
		workflowFn = peerflow.QRepFlowWorkflow
	}

	cfg.ParentMirrorName = cfg.FlowJobName

	if _, err := h.temporalClient.ExecuteWorkflow(ctx, workflowOptions, workflowFn, cfg, nil); err != nil {
		slog.Error("unable to start QRepFlow workflow",
			slog.Any("error", err), slog.String("flowName", cfg.FlowJobName))
		return nil, fmt.Errorf("unable to start QRepFlow workflow: %w", err)
	}

	return &protos.CreateQRepFlowResponse{
		WorkflowId: workflowID,
	}, nil
}

func (h *FlowRequestHandler) shutdownFlow(
	ctx context.Context,
	flowJobName string,
	deleteStats bool,
	skipDestinationDrop bool,
) error {
	workflowID, err := h.getWorkflowID(ctx, flowJobName)
	if err != nil {
		return err
	}

	logs := slog.Group("shutdown-log",
		slog.String(string(shared.FlowNameKey), flowJobName),
		slog.String("workflowId", workflowID),
	)

	if err := h.handleCancelWorkflow(ctx, workflowID, ""); err != nil {
		slog.Error("unable to cancel workflow", logs, slog.Any("error", err))
		return fmt.Errorf("unable to wait for PeerFlow workflow to close: %w", err)
	}

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

	dropFlowHandle, err := h.temporalClient.ExecuteWorkflow(ctx, workflowOptions, peerflow.DropFlowWorkflow, &protos.DropFlowInput{
		FlowJobName:           flowJobName,
		DropFlowStats:         deleteStats,
		FlowConnectionConfigs: cdcConfig,
		SkipDestinationDrop:   skipDestinationDrop,
	})
	if err != nil {
		slog.Error("unable to start DropFlow workflow", logs, slog.Any("error", err))
		return fmt.Errorf("unable to start DropFlow workflow: %w", err)
	}

	cancelCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	errLatch := concurrency.NewLatch[error]()
	go func() {
		errLatch.Set(dropFlowHandle.Get(cancelCtx, nil))
	}()

	select {
	case <-errLatch.Chan():
		if err := errLatch.Wait(); err != nil {
			slog.Error("DropFlow workflow did not execute successfully", logs, slog.Any("error", err))
			return fmt.Errorf("DropFlow workflow did not execute successfully: %w", err)
		}
	case <-time.After(5 * time.Minute):
		if err := h.handleCancelWorkflow(ctx, workflowID, ""); err != nil {
			slog.Error("unable to wait for DropFlow workflow to close", logs, slog.Any("error", err))
			return fmt.Errorf("unable to wait for DropFlow workflow to close: %w", err)
		}
	}

	return nil
}

func (h *FlowRequestHandler) FlowStateChange(
	ctx context.Context,
	req *protos.FlowStateChangeRequest,
) (*protos.FlowStateChangeResponse, error) {
	logs := slog.String("flowJobName", req.FlowJobName)
	slog.Info("FlowStateChange called", logs, slog.Any("req", req))
	if underMaintenance, err := internal.PeerDBMaintenanceModeEnabled(ctx, nil); err != nil {
		slog.Error("unable to check maintenance mode", logs, slog.Any("error", err))
		return nil, fmt.Errorf("unable to load dynamic config: %w", err)
	} else if underMaintenance {
		slog.Warn("Flow state change request denied due to maintenance", logs)
		return nil, exceptions.ErrUnderMaintenance
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
		if err := model.CDCDynamicPropertiesSignal.SignalClientWorkflow(
			ctx,
			h.temporalClient,
			workflowID,
			"",
			req.FlowConfigUpdate.GetCdcFlowConfigUpdate(),
		); err != nil {
			slog.Error("unable to signal workflow", logs, slog.Any("error", err))
			return nil, fmt.Errorf("unable to signal workflow: %w", err)
		}
	}

	slog.Info("[flow-state-change] received request", logs,
		slog.Any("requestedFlowState", req.RequestedFlowState), slog.Any("currState", currState))
	if req.RequestedFlowState != currState {
		var changeErr error
		switch req.RequestedFlowState {
		case protos.FlowStatus_STATUS_PAUSED:
			if currState == protos.FlowStatus_STATUS_RUNNING {
				changeErr = model.FlowSignal.SignalClientWorkflow(ctx, h.temporalClient, workflowID, "", model.PauseSignal)
			}
		case protos.FlowStatus_STATUS_RUNNING:
			if currState == protos.FlowStatus_STATUS_PAUSED {
				changeErr = model.FlowSignal.SignalClientWorkflow(ctx, h.temporalClient, workflowID, "", model.NoopSignal)
			}
		case protos.FlowStatus_STATUS_RESYNC:
			if currState == protos.FlowStatus_STATUS_COMPLETED {
				changeErr = h.resyncMirror(ctx, req.FlowJobName, req.DropMirrorStats)
			} else if isCDC, err := h.isCDCFlow(ctx, req.FlowJobName); err != nil {
				return nil, err
			} else if !isCDC {
				return nil, errors.New("resync is only supported for CDC mirrors")
			} else {
				slog.Info("resync requested for cdc flow", logs)
				// getting config before dropping the flow since the flow entry is deleted unconditionally
				config, err := h.getFlowConfigFromCatalog(ctx, req.FlowJobName)
				if err != nil {
					return nil, err
				}

				config.Resync = true
				config.DoInitialSnapshot = true
				// validate mirror first because once the mirror is dropped, there's no going back
				if _, err := h.ValidateCDCMirror(ctx, &protos.CreateCDCFlowRequest{
					ConnectionConfigs: config,
				}); err != nil {
					return nil, err
				}
				changeErr = model.FlowSignalStateChange.SignalClientWorkflow(ctx, h.temporalClient, workflowID, "", req)
			}
		case protos.FlowStatus_STATUS_TERMINATING, protos.FlowStatus_STATUS_TERMINATED:
			if currState != protos.FlowStatus_STATUS_TERMINATED && currState != protos.FlowStatus_STATUS_TERMINATING {
				if currState == protos.FlowStatus_STATUS_COMPLETED {
					changeErr = h.shutdownFlow(ctx, req.FlowJobName, req.DropMirrorStats, req.SkipDestinationDrop)
				} else {
					changeErr = model.FlowSignalStateChange.SignalClientWorkflow(ctx, h.temporalClient, workflowID, "", req)
				}
			}
		default:
			slog.Error("illegal state change requested", logs, slog.Any("requestedFlowState", req.RequestedFlowState),
				slog.Any("currState", currState))
			return nil, fmt.Errorf("illegal state change requested: %v, current state is: %v",
				req.RequestedFlowState, currState)
		}
		if changeErr != nil {
			slog.Error("unable to signal workflow", logs, slog.Any("error", changeErr))
			return nil, fmt.Errorf("unable to signal workflow: %w", changeErr)
		}
	}

	return &protos.FlowStateChangeResponse{}, nil
}

func (h *FlowRequestHandler) handleCancelWorkflow(ctx context.Context, workflowID, runID string) error {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	errLatch := concurrency.NewLatch[error]()
	go func() {
		errLatch.Set(h.temporalClient.CancelWorkflow(ctxWithTimeout, workflowID, runID))
	}()

	select {
	case <-errLatch.Chan():
		if err := errLatch.Wait(); err != nil {
			slog.Error(fmt.Sprintf("unable to cancel PeerFlow workflow: %s. Attempting to terminate.", err.Error()))
			terminationReason := fmt.Sprintf("workflow %s did not cancel in time.", workflowID)
			if err := h.temporalClient.TerminateWorkflow(ctx, workflowID, runID, terminationReason); err != nil {
				return fmt.Errorf("unable to terminate PeerFlow workflow: %w", err)
			}
		}
	case <-time.After(1 * time.Minute):
		slog.Error("Timeout reached while trying to cancel PeerFlow workflow. Attempting to terminate.", slog.String("workflowId", workflowID))
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
		return nil, fmt.Errorf("peer %s not found", req.PeerName)
	}

	// Check if peer name is in flows table
	peerID, err := h.getPeerID(ctx, req.PeerName)
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
func (h *FlowRequestHandler) resyncMirror(
	ctx context.Context,
	flowName string,
	dropStats bool,
) error {
	if underMaintenance, err := internal.PeerDBMaintenanceModeEnabled(ctx, nil); err != nil {
		return fmt.Errorf("unable to get maintenance mode status: %w", err)
	} else if underMaintenance {
		return exceptions.ErrUnderMaintenance
	}

	isCDC, err := h.isCDCFlow(ctx, flowName)
	if err != nil {
		return err
	}
	if !isCDC {
		return errors.New("resync is only supported for CDC mirrors")
	}
	// getting config before dropping the flow since the flow entry is deleted unconditionally
	config, err := h.getFlowConfigFromCatalog(ctx, flowName)
	if err != nil {
		return err
	}

	config.Resync = true
	config.DoInitialSnapshot = true
	// validate mirror first because once the mirror is dropped, there's no going back
	if _, err := h.ValidateCDCMirror(ctx, &protos.CreateCDCFlowRequest{
		ConnectionConfigs: config,
	}); err != nil {
		return err
	}

	if err := h.shutdownFlow(ctx, flowName, dropStats, false); err != nil {
		return err
	}

	if _, err := h.CreateCDCFlow(ctx, &protos.CreateCDCFlowRequest{
		ConnectionConfigs: config,
	}); err != nil {
		return err
	}
	return nil
}

func (h *FlowRequestHandler) GetInstanceInfo(ctx context.Context, in *protos.InstanceInfoRequest) (*protos.InstanceInfoResponse, error) {
	enabled, err := internal.PeerDBMaintenanceModeEnabled(ctx, nil)
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
	switch in.Status {
	case protos.MaintenanceStatus_MAINTENANCE_STATUS_START:
		workflowRun, err := peerflow.RunStartMaintenanceWorkflow(ctx, h.temporalClient, &protos.StartMaintenanceFlowInput{}, taskQueueId)
		if err != nil {
			return nil, err
		}
		return &protos.MaintenanceResponse{
			WorkflowId: workflowRun.GetID(),
			RunId:      workflowRun.GetRunID(),
		}, nil
	case protos.MaintenanceStatus_MAINTENANCE_STATUS_END:
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

type maintenanceWorkflowType string

const (
	startMaintenanceWorkflowType maintenanceWorkflowType = "start-maintenance"
	endMaintenanceWorkflowType   maintenanceWorkflowType = "end-maintenance"
)

func (h *FlowRequestHandler) GetMaintenanceStatus(
	ctx context.Context,
	in *protos.MaintenanceStatusRequest,
) (*protos.MaintenanceStatusResponse, error) {
	// Check if maintenance mode is enabled via dynamic setting
	maintenanceModeEnabled, err := internal.PeerDBMaintenanceModeEnabled(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to check maintenance mode: %w", err)
	}

	// Check for running maintenance workflows
	// Check if StartMaintenanceWorkflow is running and collect activity details
	var pendingActivities []*protos.MaintenanceActivityDetails
	startDesc, startWorkflowRunning, err := h.isMaintenanceWorkflowRunning(ctx, startMaintenanceWorkflowType)
	if err == nil && startWorkflowRunning {
		pendingActivities = append(pendingActivities, extractPendingActivities(startDesc)...)
	}

	// Check if EndMaintenanceWorkflow is running and collect activity details
	endDesc, endWorkflowRunning, err := h.isMaintenanceWorkflowRunning(ctx, endMaintenanceWorkflowType)
	if err == nil && endWorkflowRunning {
		pendingActivities = append(pendingActivities, extractPendingActivities(endDesc)...)
	}

	// Determine overall maintenance status and phase
	maintenanceRunning := startWorkflowRunning || endWorkflowRunning || maintenanceModeEnabled
	var phase protos.MaintenancePhase

	if startWorkflowRunning {
		phase = protos.MaintenancePhase_MAINTENANCE_PHASE_START_MAINTENANCE
	} else if endWorkflowRunning {
		phase = protos.MaintenancePhase_MAINTENANCE_PHASE_END_MAINTENANCE
	} else if maintenanceModeEnabled {
		phase = protos.MaintenancePhase_MAINTENANCE_PHASE_MAINTENANCE_MODE_ENABLED
	} else {
		phase = protos.MaintenancePhase_MAINTENANCE_PHASE_UNKNOWN
	}

	return &protos.MaintenanceStatusResponse{
		MaintenanceRunning: maintenanceRunning,
		Phase:              phase,
		PendingActivities:  pendingActivities,
	}, nil
}

func getMaintenanceWorkflowID(workflowType maintenanceWorkflowType) string {
	workflowID := string(workflowType)
	if deploymentUID := internal.PeerDBDeploymentUID(); deploymentUID != "" {
		workflowID += "-" + deploymentUID
	}
	return workflowID
}

// isMaintenanceWorkflowRunning checks if the given maintenance workflow type is currently running
func (h *FlowRequestHandler) isMaintenanceWorkflowRunning(
	ctx context.Context,
	wfType maintenanceWorkflowType,
) (*workflowservice.DescribeWorkflowExecutionResponse, bool, error) {
	startWorkflowID := getMaintenanceWorkflowID(wfType)

	desc, err := h.temporalClient.DescribeWorkflowExecution(ctx, startWorkflowID, "")
	if err != nil {
		return nil, false, err
	}

	isRunning := desc.WorkflowExecutionInfo != nil && desc.WorkflowExecutionInfo.Status == tEnums.WORKFLOW_EXECUTION_STATUS_RUNNING
	return desc, isRunning, nil
}

// extractPendingActivities extracts pending activity details from a workflow execution description
func extractPendingActivities(desc *workflowservice.DescribeWorkflowExecutionResponse) []*protos.MaintenanceActivityDetails {
	activities := make([]*protos.MaintenanceActivityDetails, len(desc.PendingActivities))

	if desc.WorkflowExecutionInfo == nil {
		return activities
	}

	// Extract pending activities from the execution info
	for i, task := range desc.PendingActivities {
		activityDetail := &protos.MaintenanceActivityDetails{
			ActivityName: task.ActivityType.Name,
			ActivityId:   task.ActivityId,
		}

		// Calculate duration if start time is available
		if task.ScheduledTime != nil {
			startTime := task.ScheduledTime.AsTime()
			duration := time.Since(startTime)
			activityDetail.ActivityDuration = durationpb.New(duration)
		}

		activityDetail.LastHeartbeat = task.LastHeartbeatTime

		// Set all heartbeat payloads if available
		if task.HeartbeatDetails != nil && len(task.HeartbeatDetails.Payloads) > 0 {
			// Convert all payloads to strings
			for _, payload := range task.HeartbeatDetails.Payloads {
				if payload != nil {
					activityDetail.HeartbeatPayloads = append(activityDetail.HeartbeatPayloads, string(payload.Data))
				}
			}
		}

		activities[i] = activityDetail
	}

	return activities
}

// SkipSnapshotWaitFlows sends a signal to skip snapshot wait for the specified flows if StartMaintenanceWorkflow is running
func (h *FlowRequestHandler) SkipSnapshotWaitFlows(
	ctx context.Context,
	in *protos.SkipSnapshotWaitFlowsRequest,
) (*protos.SkipSnapshotWaitFlowsResponse, error) {
	// Check if StartMaintenanceWorkflow is running
	_, isRunning, err := h.isMaintenanceWorkflowRunning(ctx, startMaintenanceWorkflowType)
	if err != nil {
		return &protos.SkipSnapshotWaitFlowsResponse{
			SignalSent: false,
			Message:    "Failed to check StartMaintenanceWorkflow status: " + err.Error(),
		}, nil
	}

	if !isRunning {
		return &protos.SkipSnapshotWaitFlowsResponse{
			SignalSent: false,
			Message:    "StartMaintenanceWorkflow is not currently running",
		}, nil
	}

	// Send the signal with the list of flow names using StartMaintenanceSignal
	startWorkflowID := getMaintenanceWorkflowID(startMaintenanceWorkflowType)

	// Create the signal payload
	signalPayload := &protos.StartMaintenanceSignal{
		SkippedSnapshotWaitFlows: in.FlowNames,
	}

	err = model.StartMaintenanceSignal.SignalClientWorkflow(ctx, h.temporalClient, startWorkflowID, "", signalPayload)
	if err != nil {
		return &protos.SkipSnapshotWaitFlowsResponse{
			SignalSent: false,
			Message:    "Failed to send signal: " + err.Error(),
		}, nil
	}

	return &protos.SkipSnapshotWaitFlowsResponse{
		SignalSent: true,
		Message:    fmt.Sprintf("Successfully sent skipped_snapshot_wait_flows signal for %d flows", len(in.FlowNames)),
	}, nil
}
