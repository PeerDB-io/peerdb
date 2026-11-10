package peerflow

import (
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"time"

	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peerdb/flow/activities"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// SetupFlow is the workflow that is responsible for ensuring all the
// setup is done for a sync flow to execute.
//
// The setup flow is responsible for:
//  1. Global:
//     - ensure that we are able to connect to the source and destination peers
//  2. Source Peer:
//     - setup the metadata table on the source peer
//     - initialize pullability on the source peer, as an example on postgres:
//     - ensuring the required table exists on the source peer
//     - creating the slot and publication on the source peer
//  3. Destination Peer:
//     - setup the metadata table on the destination peer
//     - creating the raw table on the destination peer
//     - creating the normalized table on the destination peer
type SetupFlowExecution struct {
	log.Logger
	tableNameMapping map[string]string
	cdcFlowName      string
	executionID      string
}

// NewSetupFlowExecution creates a new instance of SetupFlowExecution.
func NewSetupFlowExecution(ctx workflow.Context, tableNameMapping map[string]string, cdcFlowName string) *SetupFlowExecution {
	return &SetupFlowExecution{
		Logger:           log.With(workflow.GetLogger(ctx), slog.String(string(shared.FlowNameKey), cdcFlowName)),
		tableNameMapping: tableNameMapping,
		cdcFlowName:      cdcFlowName,
		executionID:      workflow.GetInfo(ctx).WorkflowExecution.ID,
	}
}

// checkConnectionsAndSetupMetadataTables checks the connections to the source and destination peers
// and ensures that the metadata tables are setup.
func (s *SetupFlowExecution) checkConnectionsAndSetupMetadataTables(
	ctx workflow.Context,
	config *protos.FlowConnectionConfigsCore,
) error {
	s.Info("checking connections for CDC flow")

	checkCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
	})

	// first check the source peer connection
	srcConnStatusFuture := workflow.ExecuteActivity(checkCtx, flowable.CheckConnection, &protos.SetupInput{
		Env:      config.Env,
		PeerName: config.SourceName,
		FlowName: config.FlowJobName,
	})
	dstSetupInput := &protos.SetupInput{
		Env:      config.Env,
		PeerName: config.DestinationName,
		FlowName: config.FlowJobName,
	}
	destConnStatusFuture := workflow.ExecuteActivity(checkCtx, flowable.CheckMetadataTables, dstSetupInput)
	if err := srcConnStatusFuture.Get(checkCtx, nil); err != nil {
		return fmt.Errorf("failed to check source peer connection: %w", err)
	}

	// then check the destination peer connection
	var destConnStatus activities.CheckMetadataTablesResult
	if err := destConnStatusFuture.Get(checkCtx, &destConnStatus); err != nil {
		return fmt.Errorf("failed to check destination peer connection: %w", err)
	}

	s.Info("ensuring metadata table exists")

	// then setup the destination peer metadata tables
	if destConnStatus.NeedsSetupMetadataTables {
		setupCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 2 * time.Minute,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval: 1 * time.Minute,
			},
		})
		if err := workflow.ExecuteActivity(setupCtx, flowable.SetupMetadataTables, dstSetupInput).Get(setupCtx, nil); err != nil {
			return fmt.Errorf("failed to setup destination peer metadata tables: %w", err)
		}
	} else {
		s.Info("destination peer metadata tables already exist")
	}

	return nil
}

// ensurePullability ensures that the source peer is pullable.
func (s *SetupFlowExecution) ensurePullability(
	ctx workflow.Context,
	config *protos.FlowConnectionConfigsCore,
	checkConstraints bool,
) (map[uint32]string, error) {
	s.Info("ensuring pullability for peer flow")

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 4 * time.Hour,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 1 * time.Minute,
		},
	})

	// create EnsurePullabilityInput for the srcTableName
	ensurePullabilityInput := &protos.EnsurePullabilityBatchInput{
		PeerName:               config.SourceName,
		FlowJobName:            s.cdcFlowName,
		SourceTableIdentifiers: slices.Sorted(maps.Keys(s.tableNameMapping)),
		CheckConstraints:       checkConstraints,
	}

	future := workflow.ExecuteActivity(ctx, flowable.EnsurePullability, ensurePullabilityInput)
	var ensurePullabilityOutput *protos.EnsurePullabilityBatchOutput
	if err := future.Get(ctx, &ensurePullabilityOutput); err != nil {
		s.Error("failed to ensure pullability for tables", err)
		return nil, fmt.Errorf("failed to ensure pullability for tables: %w", err)
	}

	if ensurePullabilityOutput == nil {
		return nil, nil
	}

	sortedTableNames := slices.Sorted(maps.Keys(ensurePullabilityOutput.TableIdentifierMapping))

	srcTableIdNameMapping := make(map[uint32]string, len(sortedTableNames))
	for _, tableName := range sortedTableNames {
		tableIdentifier := ensurePullabilityOutput.TableIdentifierMapping[tableName]
		srcTableIdNameMapping[tableIdentifier.RelId] = tableName
	}

	return srcTableIdNameMapping, nil
}

// createRawTable creates the raw table on the destination peer.
func (s *SetupFlowExecution) createRawTable(
	ctx workflow.Context,
	config *protos.FlowConnectionConfigsCore,
) error {
	s.Info("creating raw table on destination")
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 1 * time.Minute,
		},
	})

	// attempt to create the tables.
	createRawTblInput := &protos.CreateRawTableInput{
		PeerName:         config.DestinationName,
		FlowJobName:      s.cdcFlowName,
		TableNameMapping: s.tableNameMapping,
	}

	rawTblFuture := workflow.ExecuteActivity(ctx, flowable.CreateRawTable, createRawTblInput)
	if err := rawTblFuture.Get(ctx, nil); err != nil {
		return fmt.Errorf("failed to create raw table: %w", err)
	}

	return nil
}

// setupTableSchema fetches the table schema for the source tables and persists
// it to the catalog so downstream activities can use it.
func (s *SetupFlowExecution) setupTableSchema(
	ctx workflow.Context, flowConnectionConfigs *protos.FlowConnectionConfigsCore,
) error {
	s.Info("fetching table schema for peer flow")

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 1 * time.Hour,
		HeartbeatTimeout:    GetSetupFlowHeartbeatTimeout(ctx),
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 1 * time.Minute,
		},
	})

	tableSchemaInput := &protos.SetupTableSchemaBatchInput{
		PeerName:      flowConnectionConfigs.SourceName,
		TableMappings: flowConnectionConfigs.TableMappings,
		FlowName:      s.cdcFlowName,
		System:        flowConnectionConfigs.System,
		Env:           flowConnectionConfigs.Env,
		Version:       flowConnectionConfigs.Version,
	}

	if err := workflow.ExecuteActivity(ctx, flowable.SetupTableSchema, tableSchemaInput).Get(ctx, nil); err != nil {
		s.Error("failed to fetch schema for source tables", slog.Any("error", err))
		return fmt.Errorf("failed to fetch schema for source tables: %w", err)
	}
	return nil
}

// createNormalizedTables creates the normalized tables on the destination peer.
func (s *SetupFlowExecution) createNormalizedTables(
	ctx workflow.Context, flowConnectionConfigs *protos.FlowConnectionConfigsCore,
) error {
	s.Info("setting up normalized tables on destination peer", slog.String("destination", flowConnectionConfigs.DestinationName))

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 1 * time.Hour,
		HeartbeatTimeout:    GetSetupFlowHeartbeatTimeout(ctx),
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 1 * time.Minute,
		},
	})

	setupConfig := &protos.SetupNormalizedTableBatchInput{
		PeerName:          flowConnectionConfigs.DestinationName,
		TableMappings:     flowConnectionConfigs.TableMappings,
		SoftDeleteColName: flowConnectionConfigs.SoftDeleteColName,
		SyncedAtColName:   flowConnectionConfigs.SyncedAtColName,
		FlowName:          flowConnectionConfigs.FlowJobName,
		Env:               flowConnectionConfigs.Env,
		IsResync:          flowConnectionConfigs.Resync,
		Version:           flowConnectionConfigs.Version,
		Flags:             flowConnectionConfigs.Flags,
	}

	if err := workflow.ExecuteActivity(ctx, flowable.CreateNormalizedTable, setupConfig).Get(ctx, nil); err != nil {
		s.Error("failed to create normalized tables", slog.Any("error", err))
		return fmt.Errorf("failed to create normalized tables: %w", err)
	}
	return nil
}

// runPgDumpSchema runs pg_dump --schema-only on the source and pipes the output
// into psql on the destination, streaming the schema directly.
// This is only used for PG type system (PG-to-PG mirrors).
// Returns true only if the dump activity actually ran (it skips for SSH tunnel
// or non-password auth peers); callers must use this to decide whether the
// destination tables were created.
func (s *SetupFlowExecution) runPgDumpSchema(
	ctx workflow.Context,
	config *protos.FlowConnectionConfigsCore,
) (bool, error) {
	s.Info("running pg_dump schema migration from source to destination")

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 1 * time.Hour,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 1 * time.Minute,
		},
	})

	input := &protos.RunPgDumpSchemaInput{
		SourceName:      config.SourceName,
		DestinationName: config.DestinationName,
		FlowName:        config.FlowJobName,
		Env:             config.Env,
	}

	var ran bool
	if err := workflow.ExecuteActivity(ctx, flowable.RunPgDumpSchema, input).Get(ctx, &ran); err != nil {
		return false, fmt.Errorf("failed to run pg_dump schema migration: %w", err)
	}

	return ran, nil
}

// isTableAdditionChild reports whether this SetupFlow was launched as part of a
// table-addition child CDC flow. Such workflows are spawned with a parent
// workflow ID prefixed by additionalTablesCDCFlowPrefix.
func isTableAdditionChild(ctx workflow.Context) bool {
	parent := workflow.GetInfo(ctx).ParentWorkflowExecution
	if parent == nil {
		return false
	}
	return strings.HasPrefix(parent.ID, additionalTablesCDCFlowPrefix+"-")
}

// getPGAutomatedSchemaDump checks the PEERDB_PG_AUTOMATED_SCHEMA_DUMP env flag via an activity.
func (s *SetupFlowExecution) getPGAutomatedSchemaDump(ctx workflow.Context, env map[string]string) bool {
	checkCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
	})

	var enabled bool
	future := workflow.ExecuteActivity(checkCtx, flowable.PeerDBPGAutomatedSchemaDump, env)
	if err := future.Get(checkCtx, &enabled); err != nil {
		s.Warn("failed to check PEERDB_PG_AUTOMATED_SCHEMA_DUMP, defaulting to false", slog.Any("error", err))
		return false
	}
	return enabled
}

// executeSetupFlow executes the setup flow.
func (s *SetupFlowExecution) executeSetupFlow(
	ctx workflow.Context,
	config *protos.FlowConnectionConfigsCore,
) (*protos.SetupFlowOutput, error) {
	s.Info("executing setup flow")

	// first check the connectionsAndSetupMetadataTables
	if err := s.checkConnectionsAndSetupMetadataTables(ctx, config); err != nil {
		return nil, fmt.Errorf("failed to check connections and setup metadata tables: %w", err)
	}

	srcTableIdNameMapping, err := s.ensurePullability(ctx, config, !config.InitialSnapshotOnly)
	if err != nil {
		return nil, fmt.Errorf("failed to ensure pullability: %w", err)
	}

	// for initial copy only flows, we don't need to create the raw table
	if !config.InitialSnapshotOnly {
		// then create the raw table
		if err := s.createRawTable(ctx, config); err != nil {
			return nil, fmt.Errorf("failed to create raw table: %w", err)
		}
	}

	if err := s.setupTableSchema(ctx, config); err != nil {
		return nil, fmt.Errorf("failed to fetch table schema: %w", err)
	}

	// pg_dump silently no-ops for SSH tunnel / non-password-auth peers, so we
	// only skip CreateNormalizedTable when the activity reports it actually ran.
	// Skip pg_dump for resync (tables get _resync suffix and are swapped) and for
	// table-addition child workflows.
	skipCreateTables := false
	if config.System == protos.TypeSystem_PG && !config.Resync && !isTableAdditionChild(ctx) &&
		s.getPGAutomatedSchemaDump(ctx, config.Env) {
		ran, err := s.runPgDumpSchema(ctx, config)
		if err != nil {
			return nil, fmt.Errorf("failed to run pg_dump schema migration: %w", err)
		}
		skipCreateTables = ran
	}

	if skipCreateTables {
		s.Info("skipping normalized table creation, pg_dump already created tables")
	} else if err := s.createNormalizedTables(ctx, config); err != nil {
		return nil, fmt.Errorf("failed to create normalized tables: %w", err)
	}

	return &protos.SetupFlowOutput{
		SrcTableIdNameMapping: srcTableIdNameMapping,
	}, nil
}

// SetupFlowWorkflow is the workflow that sets up the flow.
func SetupFlowWorkflow(ctx workflow.Context, config *protos.FlowConnectionConfigsCore) (*protos.SetupFlowOutput, error) {
	tblNameMapping := make(map[string]string, len(config.TableMappings))
	for _, v := range config.TableMappings {
		tblNameMapping[v.SourceTableIdentifier] = v.DestinationTableIdentifier
	}

	// create the setup flow execution
	setupFlowExecution := NewSetupFlowExecution(ctx, tblNameMapping, config.FlowJobName)

	// execute the setup flow
	setupFlowOutput, err := setupFlowExecution.executeSetupFlow(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to execute setup flow: %w", err)
	}

	return setupFlowOutput, nil
}
