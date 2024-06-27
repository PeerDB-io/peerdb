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
	"github.com/PeerDB-io/peer-flow/logger"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
)

type APIServerParams struct {
	TemporalHostPort  string
	TemporalNamespace string
	TemporalCert      string
	TemporalKey       string
	Port              uint16
	GatewayPort       uint16
}

type RecryptItem struct {
	options []byte
	id      int32
}

func recryptDatabase(ctx context.Context, catalogPool *pgxpool.Pool) {
	newKeyID := peerdbenv.PeerDBCurrentEncKeyID()
	keys := peerdbenv.PeerDBEncKeys()
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

	rows, err := tx.Query(ctx, "SELECT id, options, enc_key_id FROM peers WHERE enc_key_id <> $1 FOR UPDATE", newKeyID)
	if err != nil {
		slog.Warn("recrypt failed to query, skipping", slog.Any("error", err))
		return
	}
	var todo []RecryptItem
	var id int32
	var options []byte
	var oldKeyID string
	for rows.Next() {
		if err := rows.Scan(&id, &options, &oldKeyID); err != nil {
			slog.Warn("recrypt failed to scan, skipping", slog.Any("error", err))
			continue
		}

		oldKey, err := keys.Get(oldKeyID)
		if err != nil {
			slog.Warn("recrypt failed to find key, skipping", slog.Any("error", err), slog.String("enc_key_id", oldKeyID))
			continue
		}

		if oldKey != nil {
			options, err = oldKey.Decrypt(options)
			if err != nil {
				slog.Warn("recrypt failed to decrypt, skipping", slog.Any("error", err), slog.Int64("id", int64(id)))
				continue
			}
		}

		if key != nil {
			options, err = key.Encrypt(options)
			if err != nil {
				slog.Warn("recrypt failed to encrypt, skipping", slog.Any("error", err))
				continue
			}
		}

		slog.Info("recrypting peer", slog.Int64("id", int64(id)), slog.String("oldKey", oldKeyID), slog.String("newKey", newKeyID))
		todo = append(todo, RecryptItem{id: id, options: options})
	}
	if err := rows.Err(); err != nil {
		slog.Warn("recrypt iteration failed, skipping", slog.Any("error", err))
		return
	}

	for _, item := range todo {
		if _, err := tx.Exec(ctx, "UPDATE peers SET options = $2, enc_key_id = $3 WHERE id = $1", item.id, item.options, newKeyID); err != nil {
			slog.Warn("recrypt failed to update, ignoring", slog.Any("error", err), slog.Int64("id", int64(item.id)))
			return
		}
	}

	if err := tx.Commit(ctx); err != nil {
		slog.Warn("recrypt failed to commit transaction, skipping", slog.Any("error", err))
	}
	slog.Info("recrypt finished")
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
		Logger:    slog.New(logger.NewHandler(slog.NewJSONHandler(os.Stdout, nil))),
	}
	if args.TemporalCert != "" && args.TemporalKey != "" {
		slog.Info("Using temporal certificate/key for authentication")

		certs, err := base64DecodeCertAndKey(args.TemporalCert, args.TemporalKey)
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
	go recryptDatabase(ctx, catalogPool)

	<-ctx.Done()
	grpcServer.GracefulStop()
	slog.Info("Server has been shut down gracefully. Exiting...")

	return nil
}
