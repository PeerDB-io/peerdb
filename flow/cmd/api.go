package main

import (
	"context"
	"fmt"
	"net"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"go.temporal.io/sdk/client"
)

type APIServerParams struct {
	ctx              context.Context
	Port             uint
	TemporalHostPort string
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
	flowHandler := NewFlowRequestHandler(tc)
	protos.RegisterFlowServiceServer(grpcServer, flowHandler)
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

	<-ctx.Done()

	grpcServer.GracefulStop()
	log.Println("Server has been shut down gracefully. Exiting...")

	return nil
}
