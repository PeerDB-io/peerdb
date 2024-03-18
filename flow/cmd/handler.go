package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model"
	"github.com/PeerDB-io/peer-flow/shared"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
)

// grpc server implementation
type FlowRequestHandler struct {
	temporalClient      client.Client
	pool                *pgxpool.Pool
	peerflowTaskQueueID string
	protos.UnimplementedFlowServiceServer
}

func NewFlowRequestHandler(temporalClient client.Client, pool *pgxpool.Pool, taskQueue string) *FlowRequestHandler {
	return &FlowRequestHandler{
		temporalClient:      temporalClient,
		pool:                pool,
		peerflowTaskQueueID: taskQueue,
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
	sourcePeerID, sourePeerType, srcErr := h.getPeerID(ctx, req.ConnectionConfigs.Source.Name)
	if srcErr != nil {
		return fmt.Errorf("unable to get peer id for source peer %s: %w",
			req.ConnectionConfigs.Source.Name, srcErr)
	}

	destinationPeerID, destinationPeerType, dstErr := h.getPeerID(ctx, req.ConnectionConfigs.Destination.Name)
	if dstErr != nil {
		return fmt.Errorf("unable to get peer id for target peer %s: %w",
			req.ConnectionConfigs.Destination.Name, srcErr)
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

func (h *FlowRequestHandler) createQrepJobEntry(ctx context.Context,
	req *protos.CreateQRepFlowRequest, workflowID string,
) error {
	sourcePeerName := req.QrepConfig.SourcePeer.Name
	sourcePeerID, _, srcErr := h.getPeerID(ctx, sourcePeerName)
	if srcErr != nil {
		return fmt.Errorf("unable to get peer id for source peer %s: %w",
			sourcePeerName, srcErr)
	}

	destinationPeerName := req.QrepConfig.DestinationPeer.Name
	destinationPeerID, _, dstErr := h.getPeerID(ctx, destinationPeerName)
	if dstErr != nil {
		return fmt.Errorf("unable to get peer id for target peer %s: %w",
			destinationPeerName, srcErr)
	}
	flowName := req.QrepConfig.FlowJobName
	_, err := h.pool.Exec(ctx, `INSERT INTO flows (workflow_id,name, source_peer, destination_peer, description,
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

	if req.ConnectionConfigs.SoftDeleteColName == "" {
		req.ConnectionConfigs.SoftDeleteColName = "_PEERDB_IS_DELETED"
	} else {
		// make them all uppercase
		req.ConnectionConfigs.SoftDeleteColName = strings.ToUpper(req.ConnectionConfigs.SoftDeleteColName)
	}

	if req.ConnectionConfigs.SyncedAtColName == "" {
		req.ConnectionConfigs.SyncedAtColName = "_PEERDB_SYNCED_AT"
	} else {
		// make them all uppercase
		req.ConnectionConfigs.SyncedAtColName = strings.ToUpper(req.ConnectionConfigs.SyncedAtColName)
	}

	if req.CreateCatalogEntry {
		err := h.createCdcJobEntry(ctx, req, workflowID)
		if err != nil {
			slog.Error("unable to create flow job entry", slog.Any("error", err))
			return nil, fmt.Errorf("unable to create flow job entry: %w", err)
		}
	}

	err := h.updateFlowConfigInCatalog(ctx, cfg)
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
	var cfgBytes []byte
	var err error

	cfgBytes, err = proto.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("unable to marshal flow config: %w", err)
	}

	_, err = h.pool.Exec(ctx, "UPDATE flows SET config_proto = $1 WHERE name = $2", cfgBytes, cfg.FlowJobName)
	if err != nil {
		return fmt.Errorf("unable to update flow config in catalog: %w", err)
	}

	return nil
}

func (h *FlowRequestHandler) removeFlowEntryInCatalog(
	ctx context.Context,
	flowName string,
) error {
	_, err := h.pool.Exec(ctx, "DELETE FROM flows WHERE name = $1", flowName)
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
		err := h.createQrepJobEntry(ctx, req, workflowID)
		if err != nil {
			slog.Error("unable to create flow job entry",
				slog.Any("error", err), slog.String("flowName", cfg.FlowJobName))
			return nil, fmt.Errorf("unable to create flow job entry: %w", err)
		}
	}

	state := peerflow.NewQRepFlowState()
	preColon, postColon, hasColon := strings.Cut(cfg.WatermarkColumn, "::")
	var workflowFn interface{}
	if cfg.SourcePeer.Type == protos.DBType_POSTGRES &&
		preColon == "xmin" {
		state.LastPartition.PartitionId = uuid.New().String()
		if hasColon {
			// hack to facilitate migrating from existing xmin sync
			txid, err := strconv.ParseInt(postColon, 10, 64)
			if err != nil {
				slog.Error("invalid xmin txid for xmin rep",
					slog.Any("error", err), slog.String("flowName", cfg.FlowJobName))
				return nil, fmt.Errorf("invalid xmin txid for xmin rep: %w", err)
			}
			state.LastPartition.Range = &protos.PartitionRange{
				Range: &protos.PartitionRange_IntRange{IntRange: &protos.IntPartitionRange{Start: txid}},
			}
		}

		workflowFn = peerflow.XminFlowWorkflow
	} else {
		workflowFn = peerflow.QRepFlowWorkflow
	}

	if req.QrepConfig.SyncedAtColName == "" {
		cfg.SyncedAtColName = "_PEERDB_SYNCED_AT"
	} else {
		// make them all uppercase
		cfg.SyncedAtColName = strings.ToUpper(req.QrepConfig.SyncedAtColName)
	}

	if req.QrepConfig.SourcePeer.Type == protos.DBType_SNOWFLAKE {
		sourceTables := make([]string, 0, len(req.TableMapping))
		destinationTables := make([]string, 0, len(req.TableMapping))
		for _, mapping := range req.TableMapping {
			destinationSchemaTable, err := utils.ParseSchemaTable(mapping.DestinationTableIdentifier)
			if err != nil {
				return nil, fmt.Errorf("unable to parse destination table identifier: %w", err)
			}

			sourceTables = append(sourceTables, mapping.SourceTableIdentifier)
			destinationTables = append(destinationTables, utils.PostgresSchemaTableNormalize(destinationSchemaTable))
		}
		cfg.WatermarkTable = strings.Join(sourceTables, ";")
		cfg.DestinationTableIdentifier = strings.Join(destinationTables, ";")
	}

	_, err := h.temporalClient.ExecuteWorkflow(ctx, workflowOptions, workflowFn, cfg, state)
	if err != nil {
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

// updateQRepConfigInCatalog updates the qrep config in the catalog
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

func (h *FlowRequestHandler) ShutdownFlow(
	ctx context.Context,
	req *protos.ShutdownRequest,
) (*protos.ShutdownResponse, error) {
	logs := slog.Group("shutdown-log",
		slog.String(string(shared.FlowNameKey), req.FlowJobName),
		slog.String("workflowId", req.WorkflowId),
	)

	err := h.handleCancelWorkflow(ctx, req.WorkflowId, "")
	if err != nil {
		slog.Error("unable to cancel workflow", logs, slog.Any("error", err))
		return &protos.ShutdownResponse{
			Ok:           false,
			ErrorMessage: fmt.Sprintf("unable to wait for PeerFlow workflow to close: %v", err),
		}, fmt.Errorf("unable to wait for PeerFlow workflow to close: %w", err)
	}

	if req.SourcePeer.Type == protos.DBType_POSTGRES {
		workflowID := fmt.Sprintf("%s-dropflow-%s", req.FlowJobName, uuid.New())
		workflowOptions := client.StartWorkflowOptions{
			ID:        workflowID,
			TaskQueue: h.peerflowTaskQueueID,
			SearchAttributes: map[string]interface{}{
				shared.MirrorNameSearchAttribute: req.FlowJobName,
			},
		}
		dropFlowHandle, err := h.temporalClient.ExecuteWorkflow(ctx, workflowOptions, peerflow.DropFlowWorkflow, req)
		if err != nil {
			slog.Error("unable to start DropFlow workflow",
				logs,
				slog.Any("error", err))
			return &protos.ShutdownResponse{
				Ok:           false,
				ErrorMessage: fmt.Sprintf("unable to start DropFlow workflow: %v", err),
			}, fmt.Errorf("unable to start DropFlow workflow: %w", err)
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
				return &protos.ShutdownResponse{
					Ok:           false,
					ErrorMessage: fmt.Sprintf("DropFlow workflow did not execute successfully: %v", err),
				}, fmt.Errorf("DropFlow workflow did not execute successfully: %w", err)
			}
		case <-time.After(5 * time.Minute):
			err := h.handleCancelWorkflow(ctx, workflowID, "")
			if err != nil {
				slog.Error("unable to wait for DropFlow workflow to close",
					logs,
					slog.Any("error", err),
				)
				return &protos.ShutdownResponse{
					Ok:           false,
					ErrorMessage: fmt.Sprintf("unable to wait for DropFlow workflow to close: %v", err),
				}, fmt.Errorf("unable to wait for DropFlow workflow to close: %w", err)
			}
		}
	}

	if req.RemoveFlowEntry {
		err := h.removeFlowEntryInCatalog(ctx, req.FlowJobName)
		if err != nil {
			slog.Error("unable to remove flow job entry",
				slog.String(string(shared.FlowNameKey), req.FlowJobName),
				slog.Any("error", err),
				slog.String("workflowId", req.WorkflowId))
			return &protos.ShutdownResponse{
				Ok:           false,
				ErrorMessage: err.Error(),
			}, err
		}
	}

	return &protos.ShutdownResponse{
		Ok: true,
	}, nil
}

func (h *FlowRequestHandler) FlowStateChange(
	ctx context.Context,
	req *protos.FlowStateChangeRequest,
) (*protos.FlowStateChangeResponse, error) {
	workflowID, err := h.getWorkflowID(ctx, req.FlowJobName)
	if err != nil {
		return nil, err
	}
	currState, err := h.getWorkflowStatus(ctx, workflowID)
	if err != nil {
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
			return nil, fmt.Errorf("unable to signal workflow: %w", err)
		}
	}

	// in case we only want to update properties without changing status
	if req.RequestedFlowState != protos.FlowStatus_STATUS_UNKNOWN {
		if req.RequestedFlowState == protos.FlowStatus_STATUS_PAUSED &&
			currState == protos.FlowStatus_STATUS_RUNNING {
			err = h.updateWorkflowStatus(ctx, workflowID, protos.FlowStatus_STATUS_PAUSING)
			if err != nil {
				return nil, err
			}
			err = model.FlowSignal.SignalClientWorkflow(
				ctx,
				h.temporalClient,
				workflowID,
				"",
				model.PauseSignal,
			)
		} else if req.RequestedFlowState == protos.FlowStatus_STATUS_RUNNING &&
			currState == protos.FlowStatus_STATUS_PAUSED {
			err = model.FlowSignal.SignalClientWorkflow(
				ctx,
				h.temporalClient,
				workflowID,
				"",
				model.NoopSignal,
			)
		} else if req.RequestedFlowState == protos.FlowStatus_STATUS_TERMINATED &&
			(currState != protos.FlowStatus_STATUS_TERMINATED) {
			err = h.updateWorkflowStatus(ctx, workflowID, protos.FlowStatus_STATUS_TERMINATING)
			if err != nil {
				return nil, err
			}
			_, err = h.ShutdownFlow(ctx, &protos.ShutdownRequest{
				WorkflowId:      workflowID,
				FlowJobName:     req.FlowJobName,
				SourcePeer:      req.SourcePeer,
				DestinationPeer: req.DestinationPeer,
				RemoveFlowEntry: false,
			})
		} else if req.RequestedFlowState != currState {
			return nil, fmt.Errorf("illegal state change requested: %v, current state is: %v",
				req.RequestedFlowState, currState)
		}
		if err != nil {
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

	config := req.Peer.Config
	wrongConfigResponse := &protos.CreatePeerResponse{
		Status: protos.CreatePeerStatus_FAILED,
		Message: fmt.Sprintf("invalid config for %s peer %s",
			req.Peer.Type, req.Peer.Name),
	}
	var encodedConfig []byte
	var encodingErr error
	peerType := req.Peer.Type
	switch peerType {
	case protos.DBType_POSTGRES:
		pgConfigObject, ok := config.(*protos.Peer_PostgresConfig)
		if !ok {
			return wrongConfigResponse, nil
		}
		pgConfig := pgConfigObject.PostgresConfig

		encodedConfig, encodingErr = proto.Marshal(pgConfig)

	case protos.DBType_SNOWFLAKE:
		sfConfigObject, ok := config.(*protos.Peer_SnowflakeConfig)
		if !ok {
			return wrongConfigResponse, nil
		}
		sfConfig := sfConfigObject.SnowflakeConfig
		encodedConfig, encodingErr = proto.Marshal(sfConfig)
	case protos.DBType_BIGQUERY:
		bqConfigObject, ok := config.(*protos.Peer_BigqueryConfig)
		if !ok {
			return wrongConfigResponse, nil
		}
		bqConfig := bqConfigObject.BigqueryConfig
		encodedConfig, encodingErr = proto.Marshal(bqConfig)
	case protos.DBType_SQLSERVER:
		sqlServerConfigObject, ok := config.(*protos.Peer_SqlserverConfig)
		if !ok {
			return wrongConfigResponse, nil
		}
		sqlServerConfig := sqlServerConfigObject.SqlserverConfig
		encodedConfig, encodingErr = proto.Marshal(sqlServerConfig)
	case protos.DBType_S3:
		s3ConfigObject, ok := config.(*protos.Peer_S3Config)
		if !ok {
			return wrongConfigResponse, nil
		}
		s3Config := s3ConfigObject.S3Config
		encodedConfig, encodingErr = proto.Marshal(s3Config)
	case protos.DBType_CLICKHOUSE:
		chConfigObject, ok := config.(*protos.Peer_ClickhouseConfig)

		if !ok {
			return wrongConfigResponse, nil
		}

		chConfig := chConfigObject.ClickhouseConfig
		encodedConfig, encodingErr = proto.Marshal(chConfig)
	default:
		return wrongConfigResponse, nil
	}
	if encodingErr != nil {
		slog.Error(fmt.Sprintf("failed to encode peer configuration for %s peer %s : %v",
			req.Peer.Type, req.Peer.Name, encodingErr))
		return nil, encodingErr
	}

	_, err := h.pool.Exec(ctx, "INSERT INTO peers (name, type, options) VALUES ($1, $2, $3)",
		req.Peer.Name, peerType, encodedConfig,
	)
	if err != nil {
		return &protos.CreatePeerResponse{
			Status: protos.CreatePeerStatus_FAILED,
			Message: fmt.Sprintf("failed to insert into peers table for %s peer %s: %s",
				req.Peer.Type, req.Peer.Name, err.Error()),
		}, nil
	}

	return &protos.CreatePeerResponse{
		Status:  protos.CreatePeerStatus_CREATED,
		Message: "",
	}, nil
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
	q := "SELECT workflow_id FROM flows WHERE name ILIKE $1"
	row := h.pool.QueryRow(ctx, q, flowJobName)
	var workflowID string
	if err := row.Scan(&workflowID); err != nil {
		return "", fmt.Errorf("unable to get workflowID for flow job %s: %w", flowJobName, err)
	}

	return workflowID, nil
}
