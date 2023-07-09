package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/PeerDB-io/peer-flow/activities"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/gin-gonic/gin"
	"go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
)

type APIServerParams struct {
	ctx              context.Context
	Port             uint
	CatalogJdbcURL   string
	TemporalHostPort string
}

type APIServer struct {
	ctx            context.Context
	temporalClient client.Client
	pool           *pgxpool.Pool
}

// CheckTemporalHealth checks the health of the Temporal server
func (a *APIServer) CheckTemporalHealth(reqCtx context.Context) string {
	_, err := a.temporalClient.CheckHealth(reqCtx, &client.CheckHealthRequest{})
	if err != nil {
		return fmt.Sprintf("unhealthy: %s", err)
	} else {
		return "healthy"
	}
}

// ListAllWorkflows lists all workflows
func (a *APIServer) ListAllWorkflows(reqCtx context.Context) ([]*workflow.WorkflowExecutionInfo, error) {
	var executions []*workflow.WorkflowExecutionInfo = make([]*workflow.WorkflowExecutionInfo, 0)
	var nextPageToken []byte

	for {
		resp, err := a.temporalClient.ListWorkflow(reqCtx, &workflowservice.ListWorkflowExecutionsRequest{
			PageSize:      100,
			NextPageToken: nextPageToken,
		})
		if err != nil {
			return executions, fmt.Errorf("unable to list workflows: %w", err)
		}

		executions = append(executions, resp.Executions...)
		if len(resp.NextPageToken) == 0 {
			break
		}

		nextPageToken = resp.NextPageToken
	}

	return executions, nil
}

// StartPeerFlow starts a peer flow workflow
func (a *APIServer) StartPeerFlow(reqCtx context.Context, input *peerflow.PeerFlowWorkflowInput) (string, error) {
	workflowID := fmt.Sprintf("%s-peerflow-%s", input.PeerFlowName, uuid.New())
	workflowOptions := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: shared.PeerFlowTaskQueue,
	}

	_, err := a.temporalClient.ExecuteWorkflow(
		reqCtx,                    // context
		workflowOptions,           // workflow start options
		peerflow.PeerFlowWorkflow, // workflow function
		input,                     // workflow input
	)
	if err != nil {
		return "", fmt.Errorf("unable to start PeerFlow workflow: %w", err)
	}

	return workflowID, nil
}

// StartPeerFlowWithConfig starts a peer flow workflow with the given config
func (a *APIServer) StartPeerFlowWithConfig(
	reqCtx context.Context,
	input *protos.FlowConnectionConfigs) (string, error) {
	workflowID := fmt.Sprintf("%s-peerflow-%s", input.FlowJobName, uuid.New())
	workflowOptions := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: shared.PeerFlowTaskQueue,
	}

	maxBatchSize := int(input.MaxBatchSize)
	if maxBatchSize == 0 {
		maxBatchSize = 100000
	}

	limits := &peerflow.PeerFlowLimits{
		TotalSyncFlows:      0,
		TotalNormalizeFlows: 0,
		MaxBatchSize:        maxBatchSize,
	}

	_, err := a.temporalClient.ExecuteWorkflow(
		reqCtx,                              // context
		workflowOptions,                     // workflow start options
		peerflow.PeerFlowWorkflowWithConfig, // workflow function
		input,                               // workflow input
		limits,                              // workflow limits
	)
	if err != nil {
		return "", fmt.Errorf("unable to start PeerFlow workflow: %w", err)
	}

	return workflowID, nil
}

func genConfigForQRepFlow(config *protos.QRepConfig, flowOptions map[string]interface{},
	queryString string, destinationTableIdentifier string) error {
	config.InitialCopyOnly = false
	config.MaxParallelWorkers = uint32(flowOptions["parallelism"].(float64))
	config.DestinationTableIdentifier = destinationTableIdentifier
	config.Query = queryString
	config.WatermarkColumn = flowOptions["watermark_column"].(string)
	config.WatermarkTable = flowOptions["watermark_table_name"].(string)
	config.BatchSizeInt = uint32(flowOptions["batch_size_int"].(float64))
	config.BatchDurationSeconds = uint32(flowOptions["batch_duration_timestamp"].(float64))
	config.WaitBetweenBatchesSeconds = uint32(flowOptions["refresh_interval"].(float64))
	if flowOptions["sync_data_format"].(string) == "avro" {
		config.SyncMode = protos.QRepSyncMode_QREP_SYNC_MODE_STORAGE_AVRO
		if _, ok := flowOptions["staging_path"]; ok {
			config.StagingPath = flowOptions["staging_path"].(string)
		} else {
			// if staging_path is not present, set it to empty string
			config.StagingPath = ""
		}
	} else if flowOptions["sync_data_format"].(string) == "default" {
		config.SyncMode = protos.QRepSyncMode_QREP_SYNC_MODE_MULTI_INSERT
	} else {
		return fmt.Errorf("unsupported sync data format: %s", flowOptions["sync_data_format"].(string))
	}
	if flowOptions["mode"].(string) == "append" {
		tempWriteMode := &protos.QRepWriteMode{
			WriteType: protos.QRepWriteType_QREP_WRITE_MODE_APPEND,
		}
		config.WriteMode = tempWriteMode
	} else if flowOptions["mode"].(string) == "upsert" {
		upsertKeyColumns := make([]string, 0)
		for _, column := range flowOptions["unique_key_columns"].([]interface{}) {
			upsertKeyColumns = append(upsertKeyColumns, column.(string))
		}

		tempWriteMode := &protos.QRepWriteMode{
			WriteType:        protos.QRepWriteType_QREP_WRITE_MODE_UPSERT,
			UpsertKeyColumns: upsertKeyColumns,
		}
		config.WriteMode = tempWriteMode
	} else {
		return fmt.Errorf("unsupported write mode: %s", flowOptions["mode"].(string))
	}
	return nil
}

func (a *APIServer) StartQRepFlow(reqCtx context.Context, config *protos.QRepConfig) (string, error) {
	workflowID := fmt.Sprintf("%s-qrepflow-%s", config.FlowJobName, uuid.New())
	workflowOptions := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: shared.PeerFlowTaskQueue,
	}

	if config.SourcePeer == nil || config.DestinationPeer == nil {
		sourcePeer, err := activities.FetchPeerConfig(reqCtx, a.pool, config.FlowJobName, "source_peer")
		if err != nil {
			return "", fmt.Errorf("unable to fetch source peer config: %w", err)
		}
		config.SourcePeer = sourcePeer

		destinationPeer, err := activities.FetchPeerConfig(reqCtx, a.pool, config.FlowJobName, "destination_peer")
		if err != nil {
			return "", fmt.Errorf("unable to fetch destination peer config: %w", err)
		}
		config.DestinationPeer = destinationPeer

		var destinationTableIdentifier string
		var queryString string
		var flowOptions map[string]interface{}
		row := a.pool.QueryRow(reqCtx,
			"SELECT DESTINATION_TABLE_IDENTIFIER, QUERY_STRING, FLOW_METADATA FROM FLOWS WHERE NAME = $1",
			config.FlowJobName)
		err = row.Scan(&destinationTableIdentifier, &queryString, &flowOptions)
		if err != nil {
			return "", fmt.Errorf("unable to fetch flow metadata: %w", err)
		}

		err = genConfigForQRepFlow(config, flowOptions, queryString, destinationTableIdentifier)
		if err != nil {
			return "", fmt.Errorf("unable to generate config for QRepFlow: %w", err)
		}
	}

	lastPartition := &protos.QRepPartition{
		PartitionId: "not-applicable-partition",
		Range:       nil,
	}
	numPartitionsProcessed := 0

	_, err := a.temporalClient.ExecuteWorkflow(
		reqCtx,                    // context
		workflowOptions,           // workflow start options
		peerflow.QRepFlowWorkflow, // workflow function
		config,                    // workflow input
		lastPartition,             // last partition
		numPartitionsProcessed,    // number of partitions processed
	)
	if err != nil {
		return "", fmt.Errorf("unable to start QRepFlow workflow: %w", err)
	}

	return workflowID, nil
}

// ShutdownPeerFlow signals the peer flow workflow to shutdown
func (a *APIServer) ShutdownPeerFlow(reqCtx context.Context, input *peerflow.DropFlowWorkflowInput) error {
	err := a.temporalClient.SignalWorkflow(
		reqCtx,
		input.WorkflowID,
		"",
		shared.PeerFlowSignalName,
		shared.ShutdownSignal,
	)
	if err != nil {
		return fmt.Errorf("unable to signal PeerFlow workflow: %w", err)
	}

	workflowID := fmt.Sprintf("%s-dropflow-%s", input.FlowName, uuid.New())
	workflowOptions := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: shared.PeerFlowTaskQueue,
	}
	dropFlowHandle, err := a.temporalClient.ExecuteWorkflow(
		reqCtx,                    // context
		workflowOptions,           // workflow start options
		peerflow.DropFlowWorkflow, // workflow function
		input,                     // workflow input
	)
	if err != nil {
		return fmt.Errorf("unable to start DropFlow workflow: %w", err)
	}
	if err = dropFlowHandle.Get(reqCtx, nil); err != nil {
		return fmt.Errorf("DropFlow workflow did not execute successfully: %w", err)
	}
	return nil
}

func APIMain(args *APIServerParams) error {
	ctx := args.ctx
	r := gin.Default()

	tc, err := client.Dial(client.Options{
		HostPort: args.TemporalHostPort,
	})
	if err != nil {
		return fmt.Errorf("unable to create Temporal client: %w", err)
	}

	pool, err := pgxpool.New(ctx, args.CatalogJdbcURL)
	if err != nil {
		return fmt.Errorf("unable to create database pool: %w", err)
	}

	apiServer := &APIServer{
		ctx:            ctx,
		temporalClient: tc,
		pool:           pool,
	}

	r.GET("/health", func(c *gin.Context) {
		ctx := c.Request.Context()
		c.JSON(200, gin.H{
			"status": apiServer.CheckTemporalHealth(ctx),
		})
	})

	r.GET("/flows/list", func(c *gin.Context) {
		ctx := c.Request.Context()
		workflows, err := apiServer.ListAllWorkflows(ctx)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": err.Error(),
			})
		} else {
			c.JSON(http.StatusOK, gin.H{
				"workflows": workflows,
			})
		}
	})

	r.POST("/flows/start", func(c *gin.Context) {
		// read string from request body
		var reqJSON struct {
			PeerFlowName string `json:"peer_flow_name" binding:"required"`
		}

		if err := c.ShouldBindJSON(&reqJSON); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": err.Error(),
			})
			return
		}

		input := &peerflow.PeerFlowWorkflowInput{
			CatalogJdbcURL:      args.CatalogJdbcURL,
			PeerFlowName:        reqJSON.PeerFlowName,
			TotalSyncFlows:      0,
			TotalNormalizeFlows: 0,
			MaxBatchSize:        1024 * 1024,
		}

		ctx := c.Request.Context()
		if id, err := apiServer.StartPeerFlow(ctx, input); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": err.Error(),
			})
		} else {
			c.JSON(http.StatusOK, gin.H{
				"status":      "ok",
				"workflow_id": id,
			})
		}
	})

	r.POST("/flows/shutdown", func(c *gin.Context) {
		// signal the workflow using the workflow ID (workflow_id) from json
		var reqJSON struct {
			FlowName   string `json:"flow_job_name" binding:"required"`
			WorkflowID string `json:"workflow_id" binding:"required"`
		}

		if err := c.ShouldBindJSON(&reqJSON); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": err.Error(),
			})
			return
		}

		ctx := c.Request.Context()
		input := &peerflow.DropFlowWorkflowInput{
			CatalogJdbcURL: args.CatalogJdbcURL,
			FlowName:       reqJSON.FlowName,
			WorkflowID:     reqJSON.WorkflowID,
		}
		if err := apiServer.ShutdownPeerFlow(ctx, input); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": err.Error(),
			})
		} else {
			c.JSON(http.StatusOK, gin.H{
				"status": "ok",
			})
		}
	})

	r.POST("/flows/start_with_config", func(c *gin.Context) {
		var reqJSON protos.FlowConnectionConfigs
		data, _ := c.GetRawData()

		if err := protojson.Unmarshal(data, &reqJSON); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": err.Error(),
			})
			return
		}

		ctx := c.Request.Context()
		if id, err := apiServer.StartPeerFlowWithConfig(ctx, &reqJSON); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": err.Error(),
			})
		} else {
			c.JSON(http.StatusOK, gin.H{
				"status":      "ok",
				"workflow_id": id,
			})
		}
	})

	r.POST("/qrep/start", func(c *gin.Context) {
		var reqJSON protos.QRepConfig
		data, _ := c.GetRawData()

		if err := protojson.Unmarshal(data, &reqJSON); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": err.Error(),
			})
			return
		}

		ctx := c.Request.Context()
		if id, err := apiServer.StartQRepFlow(ctx, &reqJSON); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": err.Error(),
			})
		} else {
			c.JSON(http.StatusOK, gin.H{
				"status":      "ok",
				"workflow_id": id,
			})
		}
	})

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", args.Port),
		ReadHeaderTimeout: 5 * time.Second,
		Handler:           r,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen error: %s\n", err)
		}
	}()

	<-ctx.Done()

	if err := srv.Shutdown(ctx); err != nil {
		return fmt.Errorf("server shutdown error: %w", err)
	}

	log.Println("Server has been shut down gracefully. Exiting...")
	return nil
}
