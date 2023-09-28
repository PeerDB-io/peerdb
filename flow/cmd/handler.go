package main

import (
	"context"
	"fmt"
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

	state := peerflow.NewCDCFlowState()
	_, err := h.temporalClient.ExecuteWorkflow(
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

func (h *FlowRequestHandler) CreateQRepFlow(
	ctx context.Context, req *protos.CreateQRepFlowRequest) (*protos.CreateQRepFlowResponse, error) {
	lastPartition := &protos.QRepPartition{
		PartitionId: "not-applicable-partition",
		Range:       nil,
	}

	cfg := req.QrepConfig
	workflowID := fmt.Sprintf("%s-qrepflow-%s", cfg.FlowJobName, uuid.New())
	workflowOptions := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: shared.PeerFlowTaskQueue,
	}

	numPartitionsProcessed := 0
	_, err := h.temporalClient.ExecuteWorkflow(
		ctx,                       // context
		workflowOptions,           // workflow start options
		peerflow.QRepFlowWorkflow, // workflow function
		cfg,                       // workflow input
		lastPartition,             // last partition
		numPartitionsProcessed,    // number of partitions processed
	)
	if err != nil {
		return nil, fmt.Errorf("unable to start QRepFlow workflow: %w", err)
	}

	return &protos.CreateQRepFlowResponse{
		WorflowId: workflowID,
	}, nil
}

func (h *FlowRequestHandler) ShutdownFlow(
	ctx context.Context, req *protos.ShutdownRequest) (*protos.ShutdownResponse, error) {
	err := h.temporalClient.SignalWorkflow(
		ctx,
		req.WorkflowId,
		"",
		shared.CDCFlowSignalName,
		shared.ShutdownSignal,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to signal PeerFlow workflow: %w", err)
	}

	err = h.waitForWorkflowClose(ctx, req.WorkflowId)
	if err != nil {
		return nil, fmt.Errorf("unable to wait for PeerFlow workflow to close: %w", err)
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
		return nil, fmt.Errorf("unable to start DropFlow workflow: %w", err)
	}

	if err = dropFlowHandle.Get(ctx, nil); err != nil {
		return nil, fmt.Errorf("DropFlow workflow did not execute successfully: %w", err)
	}

	return &protos.ShutdownResponse{
		Ok: true,
	}, nil
}

func (h *FlowRequestHandler) waitForWorkflowClose(ctx context.Context, workflowID string) error {
	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.InitialInterval = 5 * time.Second
	expBackoff.MaxInterval = 30 * time.Second
	expBackoff.MaxElapsedTime = 5 * time.Minute

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
		// terminate workflow if it is still running
		reason := "PeerFlow workflow did not close in time"
		err = h.temporalClient.TerminateWorkflow(ctx, workflowID, runID, reason)
		if err != nil {
			return fmt.Errorf("unable to terminate PeerFlow workflow: %w", err)
		}
	}

	return nil
}

func (h *FlowRequestHandler) ListPeers(
	ctx context.Context,
	req *protos.ListPeersRequest,
) (*protos.ListPeersResponse, error) {
	rows, err := h.pool.Query(ctx, "SELECT * FROM peers")
	if err != nil {
		return nil, fmt.Errorf("unable to query peers: %w", err)
	}
	defer rows.Close()

	peers := []*protos.Peer{}
	for rows.Next() {
		var id int
		var name string
		var peerType int
		var options []byte
		if err := rows.Scan(&id, &name, &peerType, &options); err != nil {
			return nil, fmt.Errorf("unable to scan peer row: %w", err)
		}

		dbtype := protos.DBType(peerType)
		var peer *protos.Peer
		switch dbtype {
		case protos.DBType_POSTGRES:
			var pgOptions protos.PostgresConfig
			err := proto.Unmarshal(options, &pgOptions)
			if err != nil {
				return nil, fmt.Errorf("unable to unmarshal postgres options: %w", err)
			}
			peer = &protos.Peer{
				Name:   name,
				Type:   dbtype,
				Config: &protos.Peer_PostgresConfig{PostgresConfig: &pgOptions},
			}
		case protos.DBType_BIGQUERY:
			var bqOptions protos.BigqueryConfig
			err := proto.Unmarshal(options, &bqOptions)
			if err != nil {
				return nil, fmt.Errorf("unable to unmarshal bigquery options: %w", err)
			}
			peer = &protos.Peer{
				Name:   name,
				Type:   dbtype,
				Config: &protos.Peer_BigqueryConfig{BigqueryConfig: &bqOptions},
			}
		case protos.DBType_SNOWFLAKE:
			var sfOptions protos.SnowflakeConfig
			err := proto.Unmarshal(options, &sfOptions)
			if err != nil {
				return nil, fmt.Errorf("unable to unmarshal snowflake options: %w", err)
			}
			peer = &protos.Peer{
				Name:   name,
				Type:   dbtype,
				Config: &protos.Peer_SnowflakeConfig{SnowflakeConfig: &sfOptions},
			}
		case protos.DBType_EVENTHUB:
			var ehOptions protos.EventHubConfig
			err := proto.Unmarshal(options, &ehOptions)
			if err != nil {
				return nil, fmt.Errorf("unable to unmarshal eventhub options: %w", err)
			}
			peer = &protos.Peer{
				Name:   name,
				Type:   dbtype,
				Config: &protos.Peer_EventhubConfig{EventhubConfig: &ehOptions},
			}
		case protos.DBType_SQLSERVER:
			var ssOptions protos.SqlServerConfig
			err := proto.Unmarshal(options, &ssOptions)
			if err != nil {
				return nil, fmt.Errorf("unable to unmarshal sqlserver options: %w", err)
			}
			peer = &protos.Peer{
				Name:   name,
				Type:   dbtype,
				Config: &protos.Peer_SqlserverConfig{SqlserverConfig: &ssOptions},
			}
		default:
			log.Errorf("unsupported peer type for peer '%s': %v", name, dbtype)
		}

		peers = append(peers, peer)
	}

	return &protos.ListPeersResponse{
		Peers: peers,
	}, nil
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
			Message: fmt.Sprintf("peer type is missing or your requested configuration for %s peer %s was invalidated: %s",
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
