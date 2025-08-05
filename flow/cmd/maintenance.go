package cmd

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/aws/smithy-go/ptr"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
	peerflow "github.com/PeerDB-io/peerdb/flow/workflows"
)

type MaintenanceCLIParams struct {
	TemporalHostPort                  string
	TemporalNamespace                 string
	Mode                              string
	FlowGrpcAddress                   string
	SkipIfK8sServiceMissing           string
	FlowTlsEnabled                    bool
	SkipOnApiVersionMatch             bool
	SkipOnDeploymentVersionMatch      bool
	SkipOnNoMirrors                   bool
	UseMaintenanceTaskQueue           bool
	AssumeSkippedMaintenanceWorkflows bool
}

type StartMaintenanceResult struct {
	SkippedReason    *string `json:"skippedReason,omitempty"`
	APIVersion       string  `json:"apiVersion,omitempty"`
	CLIVersion       string  `json:"cliVersion,omitempty"`
	APIDeployVersion string  `json:"apiDeployVersion,omitempty"`
	CLIDeployVersion string  `json:"cliDeployVersion,omitempty"`
	Skipped          bool    `json:"skipped,omitempty"`
}

// MaintenanceMain is the entry point for the maintenance command, requires access to Temporal client, will exit after
// running the requested maintenance workflow
func MaintenanceMain(ctx context.Context, args *MaintenanceCLIParams) error {
	slog.Info("Starting Maintenance Mode CLI")
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
	if !args.UseMaintenanceTaskQueue {
		taskQueueId = shared.PeerFlowTaskQueue
	}

	if args.Mode == "start" {
		if args.AssumeSkippedMaintenanceWorkflows {
			slog.Info("Assuming maintenance workflows were skipped")
			return WriteMaintenanceOutputToCatalog(ctx, StartMaintenanceResult{
				Skipped:       true,
				SkippedReason: ptr.String("Assumed skipped by CLI Flag"),
				CLIVersion:    internal.PeerDBVersionShaShort(),
			})
		}
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
		if err := workflowRun.Get(ctx, &output); err != nil {
			slog.Error("Error in start maintenance workflow", "error", err)
			return err
		}
		slog.Info("Start maintenance workflow completed", "output", output)
		return WriteMaintenanceOutputToCatalog(ctx, StartMaintenanceResult{
			Skipped:    false,
			CLIVersion: internal.PeerDBVersionShaShort(),
			APIVersion: output.Version,
		})
	} else if args.Mode == "end" {
		if input, err := ReadLastMaintenanceOutput(ctx); input != nil || err != nil {
			if err != nil {
				return err
			}
			slog.Info("Checking if end maintenance workflow should be skipped", "input", input)
			if input.Skipped {
				slog.Info("Skipping end maintenance workflow as start maintenance was skipped", "reason", input.SkippedReason)
				return nil
			}
		}
		workflowRun, err := peerflow.RunEndMaintenanceWorkflow(ctx, tc, &protos.EndMaintenanceFlowInput{}, taskQueueId)
		if err != nil {
			slog.Error("Error running end maintenance workflow", "error", err)
			return err
		}
		var output *protos.EndMaintenanceFlowOutput
		if err := workflowRun.Get(ctx, &output); err != nil {
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
	if args.SkipIfK8sServiceMissing != "" {
		slog.Info("Checking if k8s service exists", "service", args.SkipIfK8sServiceMissing)
		exists, err := CheckK8sServiceExistence(ctx, args.SkipIfK8sServiceMissing)
		if err != nil {
			return false, err
		}
		if !exists {
			slog.Info("Skipping maintenance workflow due to missing k8s service", "service", args.SkipIfK8sServiceMissing)
			return true, WriteMaintenanceOutputToCatalog(ctx, StartMaintenanceResult{
				Skipped:          true,
				SkippedReason:    ptr.String(fmt.Sprintf("K8s service %s missing", args.SkipIfK8sServiceMissing)),
				CLIVersion:       internal.PeerDBVersionShaShort(),
				CLIDeployVersion: internal.PeerDBDeploymentVersion(),
			})
		}
	}

	if args.SkipOnApiVersionMatch || args.SkipOnNoMirrors || args.SkipOnDeploymentVersionMatch {
		slog.Info("Checking if API version matches")
		peerFlowClient, err := constructFlowClient(args)
		if err != nil {
			return false, err
		}
		// If there is a match in BOTH PeerDB version and Deployment version, then we skip the maintenance workflow
		if args.SkipOnApiVersionMatch || args.SkipOnDeploymentVersionMatch {
			slog.Info("Checking if CLI version matches API version",
				"cliVersion", internal.PeerDBVersionShaShort(), "cliDeployVersion", internal.PeerDBDeploymentVersion())
			version, err := peerFlowClient.GetVersion(ctx, &protos.PeerDBVersionRequest{})
			if err != nil {
				return false, err
			}
			slog.Info("Got version from flow", "version", version)
			skippedReasons := make([]string, 0, 2)
			apiSkipped := !args.SkipOnApiVersionMatch
			if args.SkipOnApiVersionMatch && version.Version == internal.PeerDBVersionShaShort() {
				slog.Info("Flow API Version and Maintenance CLI version matches",
					"apiVersion", version.Version, "cliVersion", internal.PeerDBVersionShaShort())
				apiSkipped = true
				skippedReasons = append(skippedReasons, fmt.Sprintf("CLI version %s matches API version %s", internal.PeerDBVersionShaShort(),
					version.Version))
			}
			deploySkipped := !args.SkipOnDeploymentVersionMatch
			if args.SkipOnDeploymentVersionMatch &&
				(version.DeploymentVersion != nil && *version.DeploymentVersion == internal.PeerDBDeploymentVersion()) {
				slog.Info("Flow Deployment Version and Maintenance CLI Deployment version matches",
					"apiVersion", version.DeploymentVersion, "cliVersion", internal.PeerDBDeploymentVersion())
				deploySkipped = true
				skippedReasons = append(skippedReasons, fmt.Sprintf("CLI version %s matches Deployment version %s",
					internal.PeerDBDeploymentVersion(), *version.DeploymentVersion))
			}
			if apiSkipped && deploySkipped {
				slog.Info("Skipping maintenance workflow due to all versions matching",
					"apiVersion", version.Version, "cliVersion", internal.PeerDBVersionShaShort(),
					"deployApiVersion", version.DeploymentVersion, "cliDeployVersion", internal.PeerDBDeploymentVersion())
				return true, WriteMaintenanceOutputToCatalog(ctx, StartMaintenanceResult{
					Skipped:          true,
					SkippedReason:    ptr.String("Version Mismatch: " + strings.Join(skippedReasons, ", ")),
					CLIVersion:       internal.PeerDBVersionShaShort(),
					CLIDeployVersion: internal.PeerDBDeploymentVersion(),
					APIVersion:       version.Version,
					APIDeployVersion: ptr.ToString(version.DeploymentVersion),
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
				return true, WriteMaintenanceOutputToCatalog(ctx, StartMaintenanceResult{
					Skipped:       true,
					SkippedReason: ptr.String("No mirrors found"),
				})
			}
		}
	}
	return false, nil
}

func constructFlowClient(args *MaintenanceCLIParams) (protos.FlowServiceClient, error) {
	if args.FlowGrpcAddress == "" {
		return nil, errors.New("flow address is required")
	}
	slog.Info("Constructing flow client")
	transportCredentials := credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13})
	if !args.FlowTlsEnabled {
		transportCredentials = insecure.NewCredentials()
	}
	conn, err := grpc.NewClient(args.FlowGrpcAddress,
		grpc.WithTransportCredentials(transportCredentials),
	)
	if err != nil {
		return nil, fmt.Errorf("unable to dial grpc flow server: %w", err)
	}
	peerFlowClient := protos.NewFlowServiceClient(conn)
	return peerFlowClient, nil
}

func WriteMaintenanceOutputToCatalog(ctx context.Context, result StartMaintenanceResult) error {
	pool, err := internal.GetCatalogConnectionPoolFromEnv(ctx)
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx, `
	insert into maintenance.start_maintenance_outputs
		(cli_version, api_version, api_deploy_version, cli_deploy_version, skipped, skipped_reason)
	values
		($1, $2, $3, $4, $5, $6)
	`, result.CLIVersion, result.APIVersion, result.APIDeployVersion, result.CLIDeployVersion, result.Skipped, result.SkippedReason)
	return err
}

func ReadLastMaintenanceOutput(ctx context.Context) (*StartMaintenanceResult, error) {
	pool, err := internal.GetCatalogConnectionPoolFromEnv(ctx)
	if err != nil {
		return nil, err
	}
	var result StartMaintenanceResult
	if err := pool.QueryRow(ctx, `
	select cli_version, api_version, api_deploy_version, cli_deploy_version, skipped, skipped_reason
	from maintenance.start_maintenance_outputs
	order by created_at desc
	limit 1
	`).Scan(
		&result.CLIVersion,
		&result.APIVersion,
		&result.APIDeployVersion,
		&result.CLIDeployVersion,
		&result.Skipped,
		&result.SkippedReason,
	); err != nil {
		return nil, err
	}
	return &result, nil
}

func CheckK8sServiceExistence(ctx context.Context, serviceName string) (bool, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return false, err
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return false, err
	}
	_, err = clientset.CoreV1().Services(internal.GetEnvString("POD_NAMESPACE", "")).Get(ctx, serviceName, v1.GetOptions{})
	if err != nil {
		if k8sErrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
