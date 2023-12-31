package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"time"

	utils "github.com/PeerDB-io/peer-flow/connectors/utils/catalog"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
	"github.com/google/uuid"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

type APIServerParams struct {
	ctx               context.Context
	Port              uint16
	GatewayPort       uint16
	TemporalHostPort  string
	TemporalNamespace string
	TemporalCert      string
	TemporalKey       string
}

// setupGRPCGatewayServer sets up the grpc-gateway mux
func setupGRPCGatewayServer(args *APIServerParams) (*http.Server, error) {
	conn, err := grpc.DialContext(
		context.Background(),
		fmt.Sprintf("0.0.0.0:%d", args.Port),
		grpc.WithBlock(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("unable to dial grpc server: %w", err)
	}

	gwmux := runtime.NewServeMux()
	err = protos.RegisterFlowServiceHandler(context.Background(), gwmux, conn)
	if err != nil {
		return nil, fmt.Errorf("unable to register gateway: %w", err)
	}

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", args.GatewayPort),
		Handler:           gwmux,
		ReadHeaderTimeout: 5 * time.Minute,
	}
	return server, nil
}

func killExistingHeartbeatFlows(
	ctx context.Context,
	tc client.Client,
	namespace string,
	taskQueue string,
) error {
	listRes, err := tc.ListWorkflow(ctx,
		&workflowservice.ListWorkflowExecutionsRequest{
			Namespace: namespace,
			Query:     "WorkflowType = 'HeartbeatFlowWorkflow' AND TaskQueue = '" + taskQueue + "'",
		})
	if err != nil {
		return fmt.Errorf("unable to list workflows: %w", err)
	}
	slog.Info("Requesting cancellation of pre-existing heartbeat flows")
	for _, workflow := range listRes.Executions {
		slog.Info("Cancelling workflow", slog.String("workflowId", workflow.Execution.WorkflowId))
		err := tc.CancelWorkflow(ctx,
			workflow.Execution.WorkflowId, workflow.Execution.RunId)
		if err != nil && err.Error() != "workflow execution already completed" {
			return fmt.Errorf("unable to terminate workflow: %w", err)
		}
	}
	return nil
}

func APIMain(args *APIServerParams) error {
	ctx := args.ctx
	clientOptions := client.Options{
		HostPort:  args.TemporalHostPort,
		Namespace: args.TemporalNamespace,
	}
	if args.TemporalCert != "" && args.TemporalKey != "" {
		slog.Info("Using temporal certificate/key for authentication")

		certs, err := Base64DecodeCertAndKey(args.TemporalCert, args.TemporalKey)
		if err != nil {
			return fmt.Errorf("unable to base64 decode certificate and key: %w", err)
		}

		connOptions := client.ConnectionOptions{
			TLS: &tls.Config{
				Certificates: certs,
				MinVersion:   tls.VersionTLS13,
			},
		}
		clientOptions.ConnectionOptions = connOptions
	}

	tc, err := client.Dial(clientOptions)
	if err != nil {
		return fmt.Errorf("unable to create Temporal client: %w", err)
	}

	grpcServer := grpc.NewServer()

	catalogConn, err := utils.GetCatalogConnectionPoolFromEnv()
	if err != nil {
		return fmt.Errorf("unable to get catalog connection pool: %w", err)
	}

	taskQueue, err := shared.GetPeerFlowTaskQueueName(shared.PeerFlowTaskQueueID)
	if err != nil {
		return err
	}

	flowHandler := NewFlowRequestHandler(tc, catalogConn, taskQueue)

	err = killExistingHeartbeatFlows(ctx, tc, args.TemporalNamespace, taskQueue)
	if err != nil {
		return fmt.Errorf("unable to kill existing heartbeat flows: %w", err)
	}

	workflowID := fmt.Sprintf("heartbeatflow-%s", uuid.New())
	workflowOptions := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: taskQueue,
	}

	_, err = flowHandler.temporalClient.ExecuteWorkflow(
		ctx,                            // context
		workflowOptions,                // workflow start options
		peerflow.HeartbeatFlowWorkflow, // workflow function
	)
	if err != nil {
		return fmt.Errorf("unable to start heartbeat workflow: %w", err)
	}

	protos.RegisterFlowServiceServer(grpcServer, flowHandler)
	grpc_health_v1.RegisterHealthServer(grpcServer, health.NewServer())
	reflection.Register(grpcServer)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", args.Port))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	slog.Info(fmt.Sprintf("Starting API server on port %d", args.Port))
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	gateway, err := setupGRPCGatewayServer(args)
	if err != nil {
		return fmt.Errorf("unable to setup gateway server: %w", err)
	}

	slog.Info(fmt.Sprintf("Starting API gateway on port %d", args.GatewayPort))
	go func() {
		if err := gateway.ListenAndServe(); err != nil {
			log.Fatalf("failed to serve http: %v", err)
		}
	}()

	<-ctx.Done()

	grpcServer.GracefulStop()
	slog.Info("Server has been shut down gracefully. Exiting...")

	return nil
}
