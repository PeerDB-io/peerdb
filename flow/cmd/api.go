package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

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
func (a *APIServer) ListAllWorkflows(
	reqCtx context.Context,
) ([]*workflow.WorkflowExecutionInfo, error) {
	var executions []*workflow.WorkflowExecutionInfo = make([]*workflow.WorkflowExecutionInfo, 0)
	var nextPageToken []byte

	for {
		resp, err := a.temporalClient.ListWorkflow(
			reqCtx,
			&workflowservice.ListWorkflowExecutionsRequest{
				PageSize:      100,
				NextPageToken: nextPageToken,
			},
		)
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
func (a *APIServer) StartPeerFlow(
	reqCtx context.Context,
	input *peerflow.PeerFlowWorkflowInput,
) (string, error) {
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

func (a *APIServer) StartQRepFlow(
	reqCtx context.Context,
	config *protos.QRepConfig,
) (string, error) {
	workflowID := fmt.Sprintf("%s-qrepflow-%s", config.FlowJobName, uuid.New())
	workflowOptions := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: shared.PeerFlowTaskQueue,
	}

	_, err := a.temporalClient.ExecuteWorkflow(
		reqCtx,                    // context
		workflowOptions,           // workflow start options
		peerflow.QRepFlowWorkflow, // workflow function
		config,                    // workflow input
	)
	if err != nil {
		return "", fmt.Errorf("unable to start QRepFlow workflow: %w", err)
	}

	return workflowID, nil
}

// ShutdownPeerFlow signals the peer flow workflow to shutdown
func (a *APIServer) ShutdownPeerFlow(
	reqCtx context.Context,
	input *peerflow.DropFlowWorkflowInput,
) error {
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
