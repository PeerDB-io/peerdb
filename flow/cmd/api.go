package cmd

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	temporalotel "go.temporal.io/sdk/contrib/opentelemetry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/middleware"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/shared"
	peerflow "github.com/PeerDB-io/peerdb/flow/workflows"
)

type APIServerParams struct {
	TemporalHostPort  string
	TemporalNamespace string
	Port              uint16
	GatewayPort       uint16
	EnableOtelMetrics bool
}

type RecryptItem struct {
	options []byte
	id      int32
}

// updates enc_key_id by recrypting encrypted database fields with latest key
// selectSql should grab id, field, keyId respectively, taking latest keyId as parameter
// updateSql should take id, field, keyId as parameters respectively
func recryptDatabase(
	ctx context.Context,
	catalogPool shared.CatalogPool,
	tag string,
	selectSql string,
	updateSql string,
) {
	newKeyID := internal.PeerDBCurrentEncKeyID()
	keys := internal.PeerDBEncKeys(ctx)
	if newKeyID == "" {
		if len(keys) == 0 {
			slog.Warn("Encryption disabled. This is not recommended.")
		} else {
			slog.Warn("Encryption disabled, decrypting any currently encrypted configs. This is not recommended.")
		}
	}

	key, err := keys.Get(newKeyID)
	if err != nil {
		slog.Warn("recrypt failed to find key, skipping", slog.Any("error", err))
		return
	}

	tx, err := catalogPool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		slog.Warn("recrypt failed to start transaction, skipping", slog.Any("error", err))
		return
	}
	defer shared.RollbackTx(tx, slog.Default())

	rows, err := tx.Query(ctx, selectSql, newKeyID)
	if err != nil {
		slog.Warn("recrypt failed to query, skipping", slog.String("tag", tag), slog.Any("error", err))
		return
	}
	var todo []RecryptItem
	var id int32
	var options []byte
	var oldKeyID string
	for rows.Next() {
		if err := rows.Scan(&id, &options, &oldKeyID); err != nil {
			slog.Warn("recrypt failed to scan, skipping", slog.String("tag", tag), slog.Any("error", err))
			continue
		}

		oldKey, err := keys.Get(oldKeyID)
		if err != nil {
			slog.Warn("recrypt failed to find key, skipping",
				slog.String("tag", tag), slog.Any("error", err), slog.String("enc_key_id", oldKeyID))
			continue
		}

		options, err = oldKey.Decrypt(options)
		if err != nil {
			slog.Warn("recrypt failed to decrypt, skipping",
				slog.String("tag", tag), slog.Any("error", err), slog.Int64("id", int64(id)))
			continue
		}

		options, err = key.Encrypt(options)
		if err != nil {
			slog.Warn("recrypt failed to encrypt, skipping",
				slog.String("tag", tag), slog.Any("error", err), slog.Int64("id", int64(id)))
			continue
		}

		slog.Info("recrypting",
			slog.String("tag", tag), slog.Int64("id", int64(id)), slog.String("oldKey", oldKeyID), slog.String("newKey", newKeyID))
		todo = append(todo, RecryptItem{id: id, options: options})
	}
	if err := rows.Err(); err != nil {
		slog.Warn("recrypt iteration failed, skipping", slog.String("tag", tag), slog.Any("error", err))
		return
	}

	for _, item := range todo {
		if _, err := tx.Exec(ctx, updateSql, item.id, item.options, newKeyID); err != nil {
			slog.Warn("recrypt failed to update, ignoring",
				slog.String("tag", tag), slog.Any("error", err), slog.Int64("id", int64(item.id)))
			return
		}
	}

	if err := tx.Commit(ctx); err != nil {
		slog.Warn("recrypt failed to commit transaction, skipping", slog.String("tag", tag), slog.Any("error", err))
	}
	slog.Info("recrypt finished", slog.String("tag", tag))
}

// setupGRPCGatewayServer sets up the grpc-gateway mux
func setupGRPCGatewayServer(ctx context.Context, args *APIServerParams) (*http.Server, error) {
	conn, err := grpc.NewClient(
		fmt.Sprintf("0.0.0.0:%d", args.Port),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("unable to dial grpc server: %w", err)
	}

	gwmux := runtime.NewServeMux()
	if err := protos.RegisterFlowServiceHandler(ctx, gwmux, conn); err != nil {
		return nil, fmt.Errorf("unable to register gateway: %w", err)
	}

	return &http.Server{
		Addr:              fmt.Sprintf(":%d", args.GatewayPort),
		Handler:           gwmux,
		ReadHeaderTimeout: 5 * time.Minute,
	}, nil
}

func killExistingScheduleFlows(
	ctx context.Context,
	tc client.Client,
	namespace string,
	taskQueue string,
) error {
	listRes, err := tc.ListWorkflow(ctx,
		&workflowservice.ListWorkflowExecutionsRequest{
			Namespace: namespace,
			Query:     "WorkflowType = 'GlobalScheduleManagerWorkflow' AND TaskQueue = '" + taskQueue + "'",
		})
	if err != nil {
		return fmt.Errorf("unable to list workflows: %w", err)
	}
	slog.Info("Requesting cancellation of pre-existing scheduler flows")
	for _, workflow := range listRes.Executions {
		slog.Info("Cancelling workflow", slog.String("workflowId", workflow.Execution.WorkflowId))
		if err := tc.CancelWorkflow(
			ctx, workflow.Execution.WorkflowId, workflow.Execution.RunId,
		); err != nil && err.Error() != "workflow execution already completed" {
			return fmt.Errorf("unable to cancel workflow: %w", err)
		}
	}
	return nil
}

func APIMain(ctx context.Context, args *APIServerParams) error {
	clientOptions := client.Options{
		HostPort:  args.TemporalHostPort,
		Namespace: args.TemporalNamespace,
		Logger:    slog.New(shared.NewSlogHandler(slog.NewJSONHandler(os.Stdout, nil))),
	}

	metricsProvider, metricsErr := otel_metrics.SetupTemporalMetricsProvider(ctx, otel_metrics.FlowApiServiceName, args.EnableOtelMetrics)
	if metricsErr != nil {
		return metricsErr
	}
	clientOptions.MetricsHandler = temporalotel.NewMetricsHandler(temporalotel.MetricsHandlerOptions{
		Meter: metricsProvider.Meter("temporal-sdk-go"),
	})

	tc, err := setupTemporalClient(ctx, clientOptions)
	if err != nil {
		return fmt.Errorf("unable to create Temporal client: %w", err)
	}

	authGrpcMiddleware, err := middleware.AuthGrpcMiddleware([]string{
		grpc_health_v1.Health_Check_FullMethodName,
		grpc_health_v1.Health_Watch_FullMethodName,
	})
	if err != nil {
		return err
	}

	requestLoggingMiddleware := middleware.RequestLoggingMiddleWare()

	serverOptions := []grpc.ServerOption{
		// Interceptors are executed in the order they are passed to, so unauthorized requests are not logged
		grpc.ChainUnaryInterceptor(
			authGrpcMiddleware,
			requestLoggingMiddleware,
		),
	}

	componentManager, err := otel_metrics.SetupComponentMetricsProvider(
		ctx, otel_metrics.FlowApiServiceName, "grpc-api", args.EnableOtelMetrics,
	)
	if err != nil {
		return fmt.Errorf("unable to metrics provider for grpc api: %w", err)
	}
	serverOptions = append(serverOptions, grpc.StatsHandler(otelgrpc.NewServerHandler(
		otelgrpc.WithMeterProvider(componentManager),
	)))

	grpcServer := grpc.NewServer(serverOptions...)

	catalogPool, err := internal.GetCatalogConnectionPoolFromEnv(ctx)
	if err != nil {
		return fmt.Errorf("unable to get catalog connection pool: %w", err)
	}

	taskQueue := internal.PeerFlowTaskQueueName(shared.PeerFlowTaskQueue)
	flowHandler := NewFlowRequestHandler(ctx, tc, catalogPool, taskQueue)

	if err := killExistingScheduleFlows(ctx, tc, args.TemporalNamespace, taskQueue); err != nil {
		return fmt.Errorf("unable to kill existing scheduler flows: %w", err)
	}

	workflowID := fmt.Sprintf("scheduler-%s", uuid.New())
	workflowOptions := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: taskQueue,
	}

	if _, err := flowHandler.temporalClient.ExecuteWorkflow(
		ctx,
		workflowOptions,
		peerflow.GlobalScheduleManagerWorkflow,
	); err != nil {
		return fmt.Errorf("unable to start scheduler workflow: %w", err)
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

	gateway, err := setupGRPCGatewayServer(ctx, args)
	if err != nil {
		return fmt.Errorf("unable to setup gateway server: %w", err)
	}

	slog.Info(fmt.Sprintf("Starting API gateway on port %d", args.GatewayPort))
	go func() {
		if err := gateway.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("failed to serve http: %v", err)
		}
	}()

	// somewhat unrelated here, but needed a process which isn't replicated
	go recryptDatabase(
		ctx,
		catalogPool,
		"peer",
		"SELECT id, options, enc_key_id FROM peers WHERE enc_key_id <> $1 FOR UPDATE",
		"UPDATE peers SET options = $2, enc_key_id = $3 WHERE id = $1",
	)
	go recryptDatabase(
		ctx,
		catalogPool,
		"alert config",
		"SELECT id, service_config, enc_key_id FROM peerdb_stats.alerting_config WHERE enc_key_id <> $1 FOR UPDATE",
		"UPDATE peerdb_stats.alerting_config SET service_config = $2, enc_key_id = $3 WHERE id = $1",
	)

	<-ctx.Done()
	grpcServer.GracefulStop()
	slog.Info("Server has been shut down gracefully. Exiting...")

	return nil
}

func setupTemporalClient(ctx context.Context, clientOptions client.Options) (client.Client, error) {
	if internal.PeerDBTemporalEnableCertAuth() {
		slog.Info("Using temporal certificate/key for authentication")

		certs, err := parseTemporalCertAndKey(ctx)
		if err != nil {
			return nil, fmt.Errorf("unable to base64 decode certificate and key: %w", err)
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
	return tc, err
}
