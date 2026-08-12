package peerflow

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/workflows/cdc_state"
)

// TestGenerateCDCFlowReplayFixtures records real CDCFlowWorkflow histories into
// testdata/replay/*.json for TestCDCFlowWorkflowReplay. History records the
// commands the workflow issued in order and the result the activity returned.
// The replay test TestCDCFlowWorkflowReplay re-executes the workflow code
// against these recorded histories and checks for the same command sequence.
//
// Note that fake activity implementations are registered under production names
// to generate history. This is sufficient because replay never re-executes
// activities, it validates only the command sequence the workflow  code issues.
// The one exception is local activities, which is why the catalog must be running.
//
// IMPORTANT: fixtures must be generated from the workflow code that is currently
// deployed, never from the branch under test. Otherwise, replay compares new code
// against history that new code generates and trivially passes. To regenerate
// fixtures:
//  1. Check out the commit corresponding to the version currently deployed in production.
//  2. Make the desired changes to this file only (e.g. add or modify scenarios)
//  3. Start the catalog:
//     docker compose -f docker-compose-dev.yml up -d catalog
//  4. Regenerate fixtures:
//     PEERDB_GENERATE_REPLAY_FIXTURES=1 go test -count=1 -timeout 10m -run TestGenerateCDCFlowReplayFixtures ./workflows/
//  5. Check out the development branch.
//  6. Commit the fixture changes (TestCDCFlowWorkflowReplay should pass).
func TestGenerateCDCFlowReplayFixtures(t *testing.T) {
	if os.Getenv("PEERDB_GENERATE_REPLAY_FIXTURES") != "1" {
		t.Skip("set PEERDB_GENERATE_REPLAY_FIXTURES=1 to regenerate replay fixtures")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	ensureFlowsTable(ctx, t)

	devServer := startDevServer(ctx, t)
	defer func() { _ = devServer.Stop() }()
	c := devServer.Client()

	w := worker.New(c, internal.PeerFlowTaskQueueName(shared.PeerFlowTaskQueue), worker.Options{})
	w.RegisterWorkflow(CDCFlowWorkflow)
	registerFakeActivities(w)
	require.NoError(t, w.Start())
	defer w.Stop()

	require.NoError(t, os.MkdirAll(filepath.Join("testdata", "replay"), 0o755))

	t.Run("parked_resume", func(t *testing.T) {
		wfID, firstRunID := startParkedFlow(ctx, t, c, "parked-resume")
		require.NoError(t, c.SignalWorkflow(ctx, wfID, firstRunID, model.FlowSignal.Name, model.NoopSignal))
		waitContinuedAsNew(ctx, t, c, wfID, firstRunID)
		exportHistory(ctx, t, c, wfID, firstRunID, "parked_resume")
		terminateWorkflow(ctx, t, c, wfID)
	})

	t.Run("parked_config_update", func(t *testing.T) {
		wfID, firstRunID := startParkedFlow(ctx, t, c, "parked-config-update")
		update := &protos.CDCFlowConfigUpdate{BatchSize: 250, IdleTimeout: 33}
		require.NoError(t, c.SignalWorkflow(ctx, wfID, firstRunID, model.CDCDynamicPropertiesSignal.Name, update))
		waitContinuedAsNew(ctx, t, c, wfID, firstRunID)
		exportHistory(ctx, t, c, wfID, firstRunID, "parked_config_update")
		terminateWorkflow(ctx, t, c, wfID)
	})

	t.Run("running_pause", func(t *testing.T) {
		wfID, firstRunID := startRunningFlow(ctx, t, c, "running-pause")
		waitPendingActivity(ctx, t, c, wfID, firstRunID, "SyncFlow")
		require.NoError(t, c.SignalWorkflow(ctx, wfID, firstRunID, model.FlowSignal.Name, model.PauseSignal))
		waitContinuedAsNew(ctx, t, c, wfID, firstRunID)
		exportHistory(ctx, t, c, wfID, firstRunID, "running_pause")
		terminateWorkflow(ctx, t, c, wfID)
	})

	t.Run("running_terminate", func(t *testing.T) {
		wfID, firstRunID := startRunningFlow(ctx, t, c, "running-terminate")
		waitPendingActivity(ctx, t, c, wfID, firstRunID, "SyncFlow")
		req := &protos.FlowStateChangeRequest{RequestedFlowState: protos.FlowStatus_STATUS_TERMINATING}
		require.NoError(t, c.SignalWorkflow(ctx, wfID, firstRunID, model.FlowSignalStateChange.Name, req))
		waitContinuedAsNew(ctx, t, c, wfID, firstRunID)
		exportHistory(ctx, t, c, wfID, firstRunID, "running_terminate")
		terminateWorkflow(ctx, t, c, wfID)
	})

	t.Run("running_sync_finish", func(t *testing.T) {
		// the fake SyncFlow returns promptly for this flow name, exercising the
		// steady-state "sync finished -> ContinueAsNew" path
		wfID, firstRunID := startRunningFlow(ctx, t, c, "running-sync-finish")
		waitContinuedAsNew(ctx, t, c, wfID, firstRunID)
		exportHistory(ctx, t, c, wfID, firstRunID, "running_sync_finish")
		terminateWorkflow(ctx, t, c, wfID)
	})
}

func ensureFlowsTable(ctx context.Context, t *testing.T) {
	t.Helper()
	pool, err := internal.GetCatalogConnectionPoolFromEnv(ctx)
	require.NoError(t, err, "catalog Postgres unreachable; see generator doc comment for setup")
	_, err = pool.Exec(ctx,
		"CREATE TABLE IF NOT EXISTS flows (workflow_id text, name text, status integer, updated_at timestamptz DEFAULT now())")
	require.NoError(t, err)
}

func startDevServer(ctx context.Context, t *testing.T) *testsuite.DevServer {
	t.Helper()
	opts := testsuite.DevServerOptions{
		// CDCFlowWorkflow attaches the MirrorName search attribute
		// (shared.NewSearchAttributes) to child workflows it schedules; register
		// it so the server doesn't reject commands with BadSearchAttributes
		ExtraArgs: []string{"--search-attribute", "MirrorName=Text"},
	}
	if path, err := exec.LookPath("temporal"); err == nil {
		opts.ExistingPath = path
	}
	devServer, err := testsuite.StartDevServer(ctx, opts)
	require.NoError(t, err)
	return devServer
}

// registerFakeActivities registers fake CDCFlowWorkflow activities under the production name.
func registerFakeActivities(w worker.Worker) {
	w.RegisterActivityWithOptions(
		func(ctx context.Context, flowName string, srcTableIdNameMapping map[uint32]string, tableMappings []*protos.TableMapping) error {
			return nil
		},
		activity.RegisterOptions{Name: "MigratePostgresTableOIDs"},
	)
	w.RegisterActivityWithOptions(
		func(ctx context.Context, input *protos.FlowContextMetadataInput) (*protos.FlowContextMetadata, error) {
			return &protos.FlowContextMetadata{FlowName: input.FlowName}, nil
		},
		activity.RegisterOptions{Name: "GetFlowMetadata"},
	)
	w.RegisterActivityWithOptions(
		func(ctx context.Context, cfg *protos.FlowConnectionConfigsCore) error {
			return nil
		},
		activity.RegisterOptions{Name: "UpdateCDCConfigInCatalogActivity"},
	)
	w.RegisterActivityWithOptions(
		func(ctx context.Context, cfg *protos.FlowConnectionConfigsCore, opts *protos.SyncFlowOptions) error {
			// one registration serves all scenarios, so behavior is keyed on flow
			// name: running-sync-finish needs SyncFlow to complete (recording the
			// sync-finished -> ContinueAsNew path), while running-pause/-terminate
			// need it stuck in flight so the signal cancels an active sync
			if cfg.FlowJobName == fixtureFlowName("running-sync-finish") {
				time.Sleep(2 * time.Second)
				return nil
			}
			// block until canceled; heartbeats are required both by the 1-minute
			// HeartbeatTimeout and to receive the cancellation from the server
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Second):
					activity.RecordHeartbeat(ctx)
				}
			}
		},
		activity.RegisterOptions{Name: "SyncFlow"},
	)
}

func fixtureFlowName(scenario string) string {
	return "replay-fixture-" + scenario
}

func fixtureConfig(scenario string) *protos.FlowConnectionConfigsCore {
	return &protos.FlowConnectionConfigsCore{
		FlowJobName:        fixtureFlowName(scenario),
		MaxBatchSize:       100,
		IdleTimeoutSeconds: 10,
		TableMappings: []*protos.TableMapping{
			{SourceTableIdentifier: "public.replay_src", DestinationTableIdentifier: "replay_dst"},
		},
	}
}

func fixtureState(cfg *protos.FlowConnectionConfigsCore, signal model.CDCFlowSignal, status protos.FlowStatus) *cdc_state.CDCFlowWorkflowState {
	return &cdc_state.CDCFlowWorkflowState{
		ActiveSignal:      signal,
		CurrentFlowStatus: status,
		SyncFlowOptions: &protos.SyncFlowOptions{
			BatchSize:          cfg.MaxBatchSize,
			IdleTimeoutSeconds: cfg.IdleTimeoutSeconds,
			TableMappings:      cfg.TableMappings,
		},
	}
}

// startParkedFlow starts a run whose carried-over state has ActiveSignal=PauseSignal,
// mimicking the run created by ContinueAsNew when a mirror is paused, and waits for
// it to park in the pause loop.
func startParkedFlow(ctx context.Context, t *testing.T, c client.Client, scenario string) (string, string) {
	t.Helper()
	cfg := fixtureConfig(scenario)
	state := fixtureState(cfg, model.PauseSignal, protos.FlowStatus_STATUS_PAUSING)
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        fixtureFlowName(scenario),
		TaskQueue: internal.PeerFlowTaskQueueName(shared.PeerFlowTaskQueue),
	}, CDCFlowWorkflow, cfg, state)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		val, err := c.QueryWorkflow(ctx, run.GetID(), run.GetRunID(), shared.CDCFlowStateQuery)
		if err != nil {
			return false
		}
		var st cdc_state.CDCFlowWorkflowState
		if err := val.Get(&st); err != nil {
			return false
		}
		return st.CurrentFlowStatus == protos.FlowStatus_STATUS_PAUSED
	}, time.Minute, 200*time.Millisecond, "workflow never parked in pause loop")

	return run.GetID(), run.GetRunID()
}

// startRunningFlow starts a run whose carried-over state is already RUNNING, so the
// workflow skips setup/snapshot and goes straight to the main loop.
func startRunningFlow(ctx context.Context, t *testing.T, c client.Client, scenario string) (string, string) {
	t.Helper()
	cfg := fixtureConfig(scenario)
	state := fixtureState(cfg, model.NoopSignal, protos.FlowStatus_STATUS_RUNNING)
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        fixtureFlowName(scenario),
		TaskQueue: internal.PeerFlowTaskQueueName(shared.PeerFlowTaskQueue),
	}, CDCFlowWorkflow, cfg, state)
	require.NoError(t, err)
	return run.GetID(), run.GetRunID()
}

func waitPendingActivity(ctx context.Context, t *testing.T, c client.Client, wfID, runID, activityName string) {
	t.Helper()
	require.Eventually(t, func() bool {
		resp, err := c.DescribeWorkflowExecution(ctx, wfID, runID)
		if err != nil {
			return false
		}
		for _, pending := range resp.PendingActivities {
			if pending.ActivityType.GetName() == activityName {
				return true
			}
		}
		return false
	}, time.Minute, 200*time.Millisecond, "activity %s never became pending", activityName)
}

func waitContinuedAsNew(ctx context.Context, t *testing.T, c client.Client, wfID, runID string) {
	t.Helper()
	require.Eventually(t, func() bool {
		resp, err := c.DescribeWorkflowExecution(ctx, wfID, runID)
		if err != nil {
			return false
		}
		return resp.WorkflowExecutionInfo.Status == enums.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW
	}, time.Minute, 200*time.Millisecond, "run %s never continued-as-new", runID)
}

func exportHistory(ctx context.Context, t *testing.T, c client.Client, wfID, runID, name string) {
	t.Helper()
	iter := c.GetWorkflowHistory(ctx, wfID, runID, false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	var history historypb.History
	for iter.HasNext() {
		event, err := iter.Next()
		require.NoError(t, err)
		history.Events = append(history.Events, event)
	}
	require.NotEmpty(t, history.Events)

	data, err := protojson.MarshalOptions{Indent: "  "}.Marshal(&history)
	require.NoError(t, err)
	path := filepath.Join("testdata", "replay", name+".json")
	require.NoError(t, os.WriteFile(path, data, 0o600))
	t.Logf("wrote %s (%d events)", path, len(history.Events))
}

func terminateWorkflow(ctx context.Context, t *testing.T, c client.Client, wfID string) {
	t.Helper()
	if err := c.TerminateWorkflow(ctx, wfID, "", "replay fixture generation complete"); err != nil {
		t.Logf("failed to terminate %s: %v", wfID, err)
	}
}
