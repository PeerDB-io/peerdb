package main

import (
	"context"
	"fmt"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
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
