package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	utils "github.com/PeerDB-io/peer-flow/connectors/utils/catalog"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"

	"go.temporal.io/sdk/client"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

type APIServerParams struct {
	ctx              context.Context
	Port             uint
	GatewayPort      uint
	TemporalHostPort string
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

func APIMain(args *APIServerParams) error {
	ctx := args.ctx

	tc, err := client.Dial(client.Options{
		HostPort: args.TemporalHostPort,
	})
	if err != nil {
		return fmt.Errorf("unable to create Temporal client: %w", err)
	}

	grpcServer := grpc.NewServer()

	catalogConn, err := utils.GetCatalogConnectionPoolFromEnv()
	if err != nil {
		return fmt.Errorf("unable to get catalog connection pool: %w", err)
	}

	flowHandler := NewFlowRequestHandler(tc, catalogConn)
	defer flowHandler.Close()

	protos.RegisterFlowServiceServer(grpcServer, flowHandler)
	grpc_health_v1.RegisterHealthServer(grpcServer, health.NewServer())
	reflection.Register(grpcServer)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", args.Port))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	log.Printf("Starting API server on port %d", args.Port)
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	gateway, err := setupGRPCGatewayServer(args)
	if err != nil {
		return fmt.Errorf("unable to setup gateway server: %w", err)
	}

	log.Infof("Starting API gateway on port %d", args.GatewayPort)
	go func() {
		if err := gateway.ListenAndServe(); err != nil {
			log.Fatalf("failed to serve http: %v", err)
		}
	}()

	<-ctx.Done()

	grpcServer.GracefulStop()
	log.Println("Server has been shut down gracefully. Exiting...")

	return nil
}
