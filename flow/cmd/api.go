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
	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/middleware"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
)

type APIServerParams struct {
	TemporalHostPort  string
	TemporalNamespace string
	Port              uint16
	GatewayPort       uint16
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
	catalogPool *pgxpool.Pool,
	tag string,
	selectSql string,
	updateSql string,
) {
	newKeyID := peerdbenv.PeerDBCurrentEncKeyID()
	keys := peerdbenv.PeerDBEncKeys(ctx)
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
func setupGRPCGatewayServer(args *APIServerParams) (*http.Server, error) {
	conn, err := grpc.NewClient(
		fmt.Sprintf("0.0.0.0:%d", args.Port),
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
		err := tc.CancelWorkflow(ctx,
			workflow.Execution.WorkflowId, workflow.Execution.RunId)
		if err != nil && err.Error() != "workflow execution already completed" {
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

	// Interceptors are executed in the order they are passed to, so unauthorized requests are not logged
	interceptors := grpc.ChainUnaryInterceptor(
		authGrpcMiddleware,
		requestLoggingMiddleware,
	)

	grpcServer := grpc.NewServer(interceptors)

	catalogPool, err := peerdbenv.GetCatalogConnectionPoolFromEnv(ctx)
	if err != nil {
		return fmt.Errorf("unable to get catalog connection pool: %w", err)
	}

	taskQueue := peerdbenv.PeerFlowTaskQueueName(shared.PeerFlowTaskQueue)
	flowHandler := NewFlowRequestHandler(tc, catalogPool, taskQueue)

	err = killExistingScheduleFlows(ctx, tc, args.TemporalNamespace, taskQueue)
	if err != nil {
		return fmt.Errorf("unable to kill existing scheduler flows: %w", err)
	}

	workflowID := fmt.Sprintf("scheduler-%s", uuid.New())
	workflowOptions := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: taskQueue,
	}

	_, err = flowHandler.temporalClient.ExecuteWorkflow(
		ctx,
		workflowOptions,
		peerflow.GlobalScheduleManagerWorkflow,
	)
	if err != nil {
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

	gateway, err := setupGRPCGatewayServer(args)
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
	if peerdbenv.PeerDBTemporalEnableCertAuth() {
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
