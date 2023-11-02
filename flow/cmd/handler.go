package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/PeerDB-io/peer-flow/connectors"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
	backoff "github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	log "github.com/sirupsen/logrus"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/proto"
)

// grpc server implementation
type FlowRequestHandler struct {
	temporalClient client.Client
	pool           *pgxpool.Pool
	protos.UnimplementedFlowServiceServer
}

func NewFlowRequestHandler(temporalClient client.Client, pool *pgxpool.Pool) *FlowRequestHandler {
	return &FlowRequestHandler{
		temporalClient: temporalClient,
		pool:           pool,
	}
}

func (h *FlowRequestHandler) getPeerID(ctx context.Context, peerName string) (int32, int32, error) {
	var id int32
	var peerType int32
	err := h.pool.QueryRow(ctx, "SELECT id,type FROM peers WHERE name = $1", peerName).Scan(&id, &peerType)
	if err != nil {
		log.Errorf("unable to query peer id for peer %s: %s", peerName, err.Error())
		return -1, -1, fmt.Errorf("unable to query peer id for peer %s: %s", peerName, err)
	}
	return id, peerType, nil
}

func schemaForTableIdentifier(tableIdentifier string, peerDBType int32) string {
	tableIdentifierParts := strings.Split(tableIdentifier, ".")
	if len(tableIdentifierParts) == 1 && peerDBType != int32(protos.DBType_BIGQUERY) {
		tableIdentifierParts = append([]string{"public"}, tableIdentifierParts...)
	}
	return strings.Join(tableIdentifierParts, ".")
}

func (h *FlowRequestHandler) createCdcJobEntry(ctx context.Context,
	req *protos.CreateCDCFlowRequest, workflowID string) error {
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
	req *protos.CreateQRepFlowRequest, workflowID string) error {
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

// Close closes the connection pool
func (h *FlowRequestHandler) Close() {
	if h.pool != nil {
		h.pool.Close()
	}
}

func (h *FlowRequestHandler) CreateCDCFlow(
	ctx context.Context, req *protos.CreateCDCFlowRequest) (*protos.CreateCDCFlowResponse, error) {
	cfg := req.ConnectionConfigs
	workflowID := fmt.Sprintf("%s-peerflow-%s", cfg.FlowJobName, uuid.New())
	workflowOptions := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: shared.PeerFlowTaskQueue,
	}

	maxBatchSize := int(cfg.MaxBatchSize)
	if maxBatchSize == 0 {
		maxBatchSize = 100000
		cfg.MaxBatchSize = uint32(maxBatchSize)
	}

	limits := &peerflow.CDCFlowLimits{
		TotalSyncFlows:      0,
		TotalNormalizeFlows: 0,
		MaxBatchSize:        maxBatchSize,
	}

	if req.CreateCatalogEntry {
		err := h.createCdcJobEntry(ctx, req, workflowID)
		if err != nil {
			return nil, fmt.Errorf("unable to create flow job entry: %w", err)
		}
	}

	var err error
	err = h.updateFlowConfigInCatalog(cfg)
	if err != nil {
		return nil, fmt.Errorf("unable to update flow config in catalog: %w", err)
	}

	state := peerflow.NewCDCFlowWorkflowState()
	_, err = h.temporalClient.ExecuteWorkflow(
		ctx,                                // context
		workflowOptions,                    // workflow start options
		peerflow.CDCFlowWorkflowWithConfig, // workflow function
		cfg,                                // workflow input
		limits,                             // workflow limits
		state,                              // workflow state
	)
	if err != nil {
		return nil, fmt.Errorf("unable to start PeerFlow workflow: %w", err)
	}

	return &protos.CreateCDCFlowResponse{
		WorflowId: workflowID,
	}, nil
}

func (h *FlowRequestHandler) updateFlowConfigInCatalog(
	cfg *protos.FlowConnectionConfigs,
) error {
	var cfgBytes []byte
	var err error

	cfgBytes, err = proto.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("unable to marshal flow config: %w", err)
	}

	_, err = h.pool.Exec(context.Background(),
		"UPDATE flows SET config_proto = $1 WHERE name = $2",
		cfgBytes, cfg.FlowJobName)
	if err != nil {
		return fmt.Errorf("unable to update flow config in catalog: %w", err)
	}

	return nil
}

func (h *FlowRequestHandler) removeFlowEntryInCatalog(
	flowName string,
) error {
	_, err := h.pool.Exec(context.Background(),
		"DELETE FROM flows WHERE name = $1",
		flowName)
	if err != nil {
		return fmt.Errorf("unable to remove flow entry in catalog: %w", err)
	}

	return nil
}

func (h *FlowRequestHandler) CreateQRepFlow(
	ctx context.Context, req *protos.CreateQRepFlowRequest) (*protos.CreateQRepFlowResponse, error) {
	cfg := req.QrepConfig
	workflowID := fmt.Sprintf("%s-qrepflow-%s", cfg.FlowJobName, uuid.New())
	workflowOptions := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: shared.PeerFlowTaskQueue,
	}
	if req.CreateCatalogEntry {
		err := h.createQrepJobEntry(ctx, req, workflowID)
		if err != nil {
			return nil, fmt.Errorf("unable to create flow job entry: %w", err)
		}
	}

	state := peerflow.NewQRepFlowState()
	_, err := h.temporalClient.ExecuteWorkflow(
		ctx,                       // context
		workflowOptions,           // workflow start options
		peerflow.QRepFlowWorkflow, // workflow function
		cfg,                       // workflow input
		state,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to start QRepFlow workflow: %w", err)
	}

	err = h.updateQRepConfigInCatalog(cfg)
	if err != nil {
		return nil, fmt.Errorf("unable to update qrep config in catalog: %w", err)
	}

	return &protos.CreateQRepFlowResponse{
		WorflowId: workflowID,
	}, nil
}

// updateQRepConfigInCatalog updates the qrep config in the catalog
func (h *FlowRequestHandler) updateQRepConfigInCatalog(
	cfg *protos.QRepConfig,
) error {
	var cfgBytes []byte
	var err error

	cfgBytes, err = proto.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("unable to marshal qrep config: %w", err)
	}

	_, err = h.pool.Exec(context.Background(),
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
	err := h.temporalClient.SignalWorkflow(
		ctx,
		req.WorkflowId,
		"",
		shared.CDCFlowSignalName,
		shared.ShutdownSignal,
	)
	if err != nil {
		return &protos.ShutdownResponse{
			Ok:           false,
			ErrorMessage: fmt.Sprintf("unable to signal PeerFlow workflow: %v", err),
		}, fmt.Errorf("unable to signal PeerFlow workflow: %w", err)
	}

	err = h.waitForWorkflowClose(ctx, req.WorkflowId)
	if err != nil {
		return &protos.ShutdownResponse{
			Ok:           false,
			ErrorMessage: fmt.Sprintf("unable to wait for PeerFlow workflow to close: %v", err),
		}, fmt.Errorf("unable to wait for PeerFlow workflow to close: %w", err)
	}

	workflowID := fmt.Sprintf("%s-dropflow-%s", req.FlowJobName, uuid.New())
	workflowOptions := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: shared.PeerFlowTaskQueue,
	}
	dropFlowHandle, err := h.temporalClient.ExecuteWorkflow(
		ctx,                       // context
		workflowOptions,           // workflow start options
		peerflow.DropFlowWorkflow, // workflow function
		req,                       // workflow input
	)
	if err != nil {
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
			return &protos.ShutdownResponse{
				Ok:           false,
				ErrorMessage: fmt.Sprintf("DropFlow workflow did not execute successfully: %v", err),
			}, fmt.Errorf("DropFlow workflow did not execute successfully: %w", err)
		}
	case <-time.After(1 * time.Minute):
		err := h.handleWorkflowNotClosed(ctx, workflowID, "")
		if err != nil {
			return &protos.ShutdownResponse{
				Ok:           false,
				ErrorMessage: fmt.Sprintf("unable to wait for DropFlow workflow to close: %v", err),
			}, fmt.Errorf("unable to wait for DropFlow workflow to close: %w", err)
		}
	}

	if req.RemoveFlowEntry {
		delErr := h.removeFlowEntryInCatalog(req.FlowJobName)
		if delErr != nil {
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

func (h *FlowRequestHandler) PauseFlow(
	ctx context.Context,
	req *protos.PauseRequest,
) (*protos.PauseResponse, error) {
	err := h.temporalClient.SignalWorkflow(
		ctx,
		req.WorkflowId,
		"",
		shared.CDCFlowSignalName,
		shared.PauseSignal,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to signal PeerFlow workflow: %w", err)
	}

	return &protos.PauseResponse{
		Ok: true,
	}, nil
}

func (h *FlowRequestHandler) waitForWorkflowClose(ctx context.Context, workflowID string) error {
	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.InitialInterval = 3 * time.Second
	expBackoff.MaxInterval = 10 * time.Second
	expBackoff.MaxElapsedTime = 1 * time.Minute

	// empty will terminate the latest run
	runID := ""

	operation := func() error {
		workflowRes, err := h.temporalClient.DescribeWorkflowExecution(ctx, workflowID, runID)
		if err != nil {
			// Permanent error will stop the retries
			return backoff.Permanent(fmt.Errorf("unable to describe PeerFlow workflow: %w", err))
		}

		if workflowRes.WorkflowExecutionInfo.CloseTime != nil {
			return nil
		}

		return fmt.Errorf("workflow - %s not closed yet: %v", workflowID, workflowRes)
	}

	err := backoff.Retry(operation, expBackoff)
	if err != nil {
		return h.handleWorkflowNotClosed(ctx, workflowID, runID)
	}

	return nil
}

func (h *FlowRequestHandler) handleWorkflowNotClosed(ctx context.Context, workflowID, runID string) error {
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
			log.Errorf("unable to cancel PeerFlow workflow: %s. Attempting to terminate.", err.Error())
			terminationReason := fmt.Sprintf("workflow %s did not cancel in time.", workflowID)
			if err = h.temporalClient.TerminateWorkflow(ctx, workflowID, runID, terminationReason); err != nil {
				return fmt.Errorf("unable to terminate PeerFlow workflow: %w", err)
			}
		}
	case <-time.After(1 * time.Minute):
		// If 1 minute has passed and we haven't received an error, terminate the workflow
		log.Errorf("Timeout reached while trying to cancel PeerFlow workflow. Attempting to terminate.")
		terminationReason := fmt.Sprintf("workflow %s did not cancel in time.", workflowID)
		if err := h.temporalClient.TerminateWorkflow(ctx, workflowID, runID, terminationReason); err != nil {
			return fmt.Errorf("unable to terminate PeerFlow workflow: %w", err)
		}
	}

	return nil
}

func (h *FlowRequestHandler) ValidatePeer(
	ctx context.Context,
	req *protos.ValidatePeerRequest,
) (*protos.ValidatePeerResponse, error) {
	if req.Peer == nil {
		return &protos.ValidatePeerResponse{
			Status:  protos.ValidatePeerStatus_INVALID,
			Message: "no peer provided",
		}, nil
	}

	if len(req.Peer.Name) == 0 {
		return &protos.ValidatePeerResponse{
			Status:  protos.ValidatePeerStatus_INVALID,
			Message: "no peer name provided",
		}, nil
	}

	conn, err := connectors.GetConnector(ctx, req.Peer)
	if err != nil {
		return &protos.ValidatePeerResponse{
			Status: protos.ValidatePeerStatus_INVALID,
			Message: fmt.Sprintf("peer type is missing or "+
				"your requested configuration for %s peer %s was invalidated: %s",
				req.Peer.Type, req.Peer.Name, err),
		}, nil
	}

	status := conn.ConnectionActive()
	if !status {
		return &protos.ValidatePeerResponse{
			Status: protos.ValidatePeerStatus_INVALID,
			Message: fmt.Sprintf("failed to establish active connection to %s peer %s.",
				req.Peer.Type, req.Peer.Name),
		}, nil
	}

	return &protos.ValidatePeerResponse{
		Status: protos.ValidatePeerStatus_VALID,
		Message: fmt.Sprintf("%s peer %s is valid",
			req.Peer.Type, req.Peer.Name),
	}, nil
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

	default:
		return wrongConfigResponse, nil
	}
	if encodingErr != nil {
		log.Errorf("failed to encode peer configuration for %s peer %s : %v",
			req.Peer.Type, req.Peer.Name, encodingErr)
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

	var inMirror int64
	queryErr := h.pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM flows WHERE source_peer=$1 or destination_peer=$2",
		peerID, peerID).Scan(&inMirror)
	if queryErr != nil {
		return &protos.DropPeerResponse{
			Ok:           false,
			ErrorMessage: fmt.Sprintf("Failed to check for existing mirrors with peer %s: %v", req.PeerName, queryErr),
		}, fmt.Errorf("failed to check for existing mirrors with peer %s", req.PeerName)
	}

	if inMirror != 0 {
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
