package peerflow

import (
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"time"

	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"

	"github.com/PeerDB-io/peer-flow/activities"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
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
	config *protos.FlowConnectionConfigs,
) error {
	s.Info("checking connections for CDC flow")

	checkCtx := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
		StartToCloseTimeout: time.Minute,
	})

	// first check the source peer connection
	srcConnStatusFuture := workflow.ExecuteLocalActivity(checkCtx, flowable.CheckConnection, &protos.SetupInput{
		Env:      config.Env,
		PeerName: config.SourceName,
		FlowName: config.FlowJobName,
	})
	var srcConnStatus activities.CheckConnectionResult
	if err := srcConnStatusFuture.Get(checkCtx, &srcConnStatus); err != nil {
		return fmt.Errorf("failed to check source peer connection: %w", err)
	}

	dstSetupInput := &protos.SetupInput{
		Env:      config.Env,
		PeerName: config.DestinationName,
		FlowName: config.FlowJobName,
	}

	// then check the destination peer connection
	destConnStatusFuture := workflow.ExecuteLocalActivity(checkCtx, flowable.CheckConnection, dstSetupInput)
	var destConnStatus activities.CheckConnectionResult
	if err := destConnStatusFuture.Get(checkCtx, &destConnStatus); err != nil {
		return fmt.Errorf("failed to check destination peer connection: %w", err)
	}

	s.Info("ensuring metadata table exists")

	// then setup the destination peer metadata tables
	if destConnStatus.NeedsSetupMetadataTables {
		setupCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 2 * time.Minute,
		})
		fDst := workflow.ExecuteActivity(setupCtx, flowable.SetupMetadataTables, dstSetupInput)
		if err := fDst.Get(setupCtx, nil); err != nil {
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
	config *protos.FlowConnectionConfigs,
	checkConstraints bool,
) (map[uint32]string, error) {
	s.Info("ensuring pullability for peer flow")

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 4 * time.Hour,
	})
	srcTableIdNameMapping := make(map[uint32]string)

	srcTblIdentifiers := slices.Sorted(maps.Keys(s.tableNameMapping))

	// create EnsurePullabilityInput for the srcTableName
	ensurePullabilityInput := &protos.EnsurePullabilityBatchInput{
		PeerName:               config.SourceName,
		FlowJobName:            s.cdcFlowName,
		SourceTableIdentifiers: srcTblIdentifiers,
		CheckConstraints:       checkConstraints,
	}

	future := workflow.ExecuteActivity(ctx, flowable.EnsurePullability, ensurePullabilityInput)
	var ensurePullabilityOutput *protos.EnsurePullabilityBatchOutput
	if err := future.Get(ctx, &ensurePullabilityOutput); err != nil {
		s.Error("failed to ensure pullability for tables: ", err)
		return nil, fmt.Errorf("failed to ensure pullability for tables: %w", err)
	}

	sortedTableNames := slices.Sorted(maps.Keys(ensurePullabilityOutput.TableIdentifierMapping))

	for _, tableName := range sortedTableNames {
		tableIdentifier := ensurePullabilityOutput.TableIdentifierMapping[tableName]
		srcTableIdNameMapping[tableIdentifier.RelId] = tableName
	}

	return srcTableIdNameMapping, nil
}

// createRawTable creates the raw table on the destination peer.
func (s *SetupFlowExecution) createRawTable(
	ctx workflow.Context,
	config *protos.FlowConnectionConfigs,
) error {
	s.Info("creating raw table on destination")
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
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

// fetchTableSchemaAndSetupNormalizedTables fetches the table schema for the source table and
// sets up the normalized tables on the destination peer.
func (s *SetupFlowExecution) fetchTableSchemaAndSetupNormalizedTables(
	ctx workflow.Context, flowConnectionConfigs *protos.FlowConnectionConfigs,
) (map[string]*protos.TableSchema, error) {
	s.Info("fetching table schema for peer flow")

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 1 * time.Hour,
		HeartbeatTimeout:    time.Minute,
	})

	sourceTables := slices.Sorted(maps.Keys(s.tableNameMapping))

	tableSchemaInput := &protos.GetTableSchemaBatchInput{
		PeerName:         flowConnectionConfigs.SourceName,
		TableIdentifiers: sourceTables,
		FlowName:         s.cdcFlowName,
		System:           flowConnectionConfigs.System,
		Env:              flowConnectionConfigs.Env,
	}

	future := workflow.ExecuteActivity(ctx, flowable.GetTableSchema, tableSchemaInput)

	var tblSchemaOutput *protos.GetTableSchemaBatchOutput
	if err := future.Get(ctx, &tblSchemaOutput); err != nil {
		s.Error("failed to fetch schema for source tables", slog.Any("error", err))
		return nil, fmt.Errorf("failed to fetch schema for source table %s: %w", sourceTables, err)
	}

	s.Info("setting up normalized tables for peer flow")
	normalizedTableMapping := shared.BuildProcessedSchemaMapping(flowConnectionConfigs.TableMappings,
		tblSchemaOutput.TableNameSchemaMapping, s.Logger)

	// now setup the normalized tables on the destination peer
	setupConfig := &protos.SetupNormalizedTableBatchInput{
		PeerName:               flowConnectionConfigs.DestinationName,
		TableNameSchemaMapping: normalizedTableMapping,
		SoftDeleteColName:      flowConnectionConfigs.SoftDeleteColName,
		SyncedAtColName:        flowConnectionConfigs.SyncedAtColName,
		FlowName:               flowConnectionConfigs.FlowJobName,
		Env:                    flowConnectionConfigs.Env,
		IsResync:               flowConnectionConfigs.Resync,
	}

	future = workflow.ExecuteActivity(ctx, flowable.CreateNormalizedTable, setupConfig)
	if err := future.Get(ctx, nil); err != nil {
		s.Error("failed to create normalized tables: ", err)
		return nil, fmt.Errorf("failed to create normalized tables: %w", err)
	}

	s.Info("finished setting up normalized tables for peer flow")
	return normalizedTableMapping, nil
}

// executeSetupFlow executes the setup flow.
func (s *SetupFlowExecution) executeSetupFlow(
	ctx workflow.Context,
	config *protos.FlowConnectionConfigs,
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

	// then fetch the table schema and setup the normalized tables
	tableNameSchemaMapping, err := s.fetchTableSchemaAndSetupNormalizedTables(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch table schema and setup normalized tables: %w", err)
	}

	return &protos.SetupFlowOutput{
		SrcTableIdNameMapping:  srcTableIdNameMapping,
		TableNameSchemaMapping: tableNameSchemaMapping,
	}, nil
}

// SetupFlowWorkflow is the workflow that sets up the flow.
func SetupFlowWorkflow(ctx workflow.Context,
	config *protos.FlowConnectionConfigs,
) (*protos.SetupFlowOutput, error) {
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
