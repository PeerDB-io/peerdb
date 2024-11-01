package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"go.temporal.io/sdk/client"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
)

type MaintenanceCLIParams struct {
	TemporalHostPort  string
	TemporalNamespace string
	FlowType          string
}

// MaintenanceMain is the entry point for the maintenance command, requires access to Temporal client, will exit after
// running the requested maintenance workflow
func MaintenanceMain(ctx context.Context, args *MaintenanceCLIParams) error {
	clientOptions := client.Options{
		HostPort:  args.TemporalHostPort,
		Namespace: args.TemporalNamespace,
		Logger:    slog.New(shared.NewSlogHandler(slog.NewJSONHandler(os.Stdout, nil))),
	}

	tc, err := setupTemporalClient(ctx, clientOptions)
	if err != nil {
		return fmt.Errorf("unable to create Temporal client: %w", err)
	}

	switch args.FlowType {
	case "start":
		slog.Info("Running start maintenance workflow")
		workflowRun, err := peerflow.RunStartMaintenanceWorkflow(ctx, tc, &protos.StartMaintenanceFlowInput{})
		if err != nil {
			slog.Error("Error running start maintenance workflow", "error", err)
			return err
		}
		var output *protos.StartMaintenanceFlowOutput
		err = workflowRun.Get(ctx, &output)
		if err != nil {
			slog.Error("Error in start maintenance workflow", "error", err)
			return err
		}
		slog.Info("Start maintenance workflow completed", "output", output)
		return nil
	case "end":
		slog.Info("Running end maintenance workflow")
		workflowRun, err := peerflow.RunEndMaintenanceWorkflow(ctx, tc, &protos.EndMaintenanceFlowInput{})
		if err != nil {
			slog.Error("Error running end maintenance workflow", "error", err)
			return err
		}
		var output *protos.EndMaintenanceFlowOutput
		err = workflowRun.Get(ctx, &output)
		if err != nil {
			slog.Error("Error in end maintenance workflow", "error", err)
			return err
		}
		slog.Info("End maintenance workflow completed", "output", output)
	default:
		return fmt.Errorf("unknown flow type %s", args.FlowType)
	}
	slog.Info("Maintenance workflow completed with type", "type", args.FlowType)
	return nil
}
