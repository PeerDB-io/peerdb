package cmd

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"go.temporal.io/sdk/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/peerdbenv"
	"github.com/PeerDB-io/peer-flow/shared"
	peerflow "github.com/PeerDB-io/peer-flow/workflows"
)

type MaintenanceCLIParams struct {
	TemporalHostPort         string
	TemporalNamespace        string
	Mode                     string
	OutputFile               string
	InputFile                string
	FlowGrpcAddress          string
	FlowTlsEnabled           bool
	SkipOnApiVersionMatch    bool
	SkipOnNoMirrors          bool
	UserMaintenanceTaskQueue bool
}

type StartMaintenanceResult struct {
	APIVersion    string `json:"apiVersion,omitempty"`
	CLIVersion    string `json:"cliVersion,omitempty"`
	SkippedReason string `json:"skippedReason,omitempty"`
	Skipped       bool   `json:"skipped,omitempty"`
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

	taskQueueId := shared.MaintenanceFlowTaskQueue
	if !args.UserMaintenanceTaskQueue {
		taskQueueId = shared.PeerFlowTaskQueue
	}

	if args.Mode == "start" {
		skipped, err := skipStartMaintenanceIfNeeded(ctx, args)
		if err != nil {
			return err
		}
		if skipped {
			return nil
		}
		slog.Info("Running start maintenance workflow")
		workflowRun, err := peerflow.RunStartMaintenanceWorkflow(ctx, tc, &protos.StartMaintenanceFlowInput{}, taskQueueId)
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
		return WriteMaintenanceOutput(args.OutputFile, StartMaintenanceResult{
			Skipped:    false,
			CLIVersion: peerdbenv.PeerDBVersionShaShort(),
		})
	} else if args.Mode == "end" {
		if input, err := ReadMaintenanceInput(args.InputFile); input != nil || err != nil {
			if err != nil {
				return err
			}
			if input.Skipped {
				slog.Info("Skipping end maintenance workflow as start maintenance was skipped", "reason", input.SkippedReason)
				return nil
			}
		}
		slog.Info("Running end maintenance workflow")
		workflowRun, err := peerflow.RunEndMaintenanceWorkflow(ctx, tc, &protos.EndMaintenanceFlowInput{}, taskQueueId)
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
	} else {
		return fmt.Errorf("unknown flow type %s", args.Mode)
	}
	slog.Info("Maintenance workflow completed with type", "type", args.Mode)
	return nil
}

func skipStartMaintenanceIfNeeded(ctx context.Context, args *MaintenanceCLIParams) (bool, error) {
	if args.SkipOnApiVersionMatch || args.SkipOnNoMirrors {
		if args.FlowGrpcAddress == "" {
			return false, errors.New("flow address is required when skipping based on API")
		}
		slog.Info("Constructing flow client")
		transportCredentials := credentials.NewTLS(&tls.Config{
			MinVersion: tls.VersionTLS12,
		})
		if !args.FlowTlsEnabled {
			transportCredentials = insecure.NewCredentials()
		}
		conn, err := grpc.NewClient(args.FlowGrpcAddress,
			grpc.WithTransportCredentials(transportCredentials),
		)
		if err != nil {
			return false, fmt.Errorf("unable to dial grpc flow server: %w", err)
		}
		peerFlowClient := protos.NewFlowServiceClient(conn)
		if args.SkipOnApiVersionMatch {
			slog.Info("Checking if CLI version matches API version", "cliVersion", peerdbenv.PeerDBVersionShaShort())
			version, err := peerFlowClient.GetVersion(ctx, &protos.PeerDBVersionRequest{})
			if err != nil {
				return false, err
			}
			slog.Info("Got version from flow", "version", version.Version)
			if version.Version == peerdbenv.PeerDBVersionShaShort() {
				slog.Info("Skipping maintenance workflow due to matching versions")
				return true, WriteMaintenanceOutput(args.OutputFile, StartMaintenanceResult{
					Skipped:       true,
					SkippedReason: fmt.Sprintf("CLI version %s matches API version %s", peerdbenv.PeerDBVersionShaShort(), version.Version),
					APIVersion:    version.Version,
					CLIVersion:    peerdbenv.PeerDBVersionShaShort(),
				})
			}
		}
		if args.SkipOnNoMirrors {
			slog.Info("Checking if there are any mirrors")
			mirrors, err := peerFlowClient.ListMirrors(ctx, &protos.ListMirrorsRequest{})
			if err != nil {
				return false, err
			}
			slog.Info("Got mirrors from flow", "mirrors", mirrors.Mirrors)
			if len(mirrors.Mirrors) == 0 {
				slog.Info("Skipping maintenance workflow due to no mirrors")
				return true, WriteMaintenanceOutput(args.OutputFile, StartMaintenanceResult{
					Skipped:       true,
					SkippedReason: "No mirrors found",
				})
			}
		}
	}
	return false, nil
}

func WriteMaintenanceOutput(outputFile string, result StartMaintenanceResult) error {
	if outputFile == "" {
		return nil
	}
	slog.Info("Writing maintenance result to file", "file", outputFile, "result", result)
	f, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("unable to create output file: %w", err)
	}
	defer f.Close()
	// marshall to json
	marshalledResult, err := json.Marshal(result)
	if err != nil {
		return err
	}
	_, err = f.Write(marshalledResult)
	if err != nil {
		return fmt.Errorf("unable to write to output file: %w", err)
	}

	return nil
}

func ReadMaintenanceInput(inputFile string) (*StartMaintenanceResult, error) {
	if inputFile == "" {
		return nil, nil
	}
	slog.Info("Reading maintenance input from file", "file", inputFile)
	f, err := os.Open(inputFile)
	if err != nil {
		return nil, fmt.Errorf("unable to open input file: %w", err)
	}
	defer f.Close()
	var params StartMaintenanceResult
	decoder := json.NewDecoder(f)
	err = decoder.Decode(&params)
	if err != nil {
		return nil, fmt.Errorf("unable to decode input file: %w", err)
	}
	return &params, nil
}
