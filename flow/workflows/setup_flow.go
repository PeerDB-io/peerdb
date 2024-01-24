package peerflow

import (
	"fmt"
	"slices"
	"sort"
	"time"

	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"
	"golang.org/x/exp/maps"

	"github.com/PeerDB-io/peer-flow/activities"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
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
	tableNameMapping map[string]string
	cdcFlowName      string
	executionID      string
	logger           log.Logger
}

// NewSetupFlowExecution creates a new instance of SetupFlowExecution.
func NewSetupFlowExecution(ctx workflow.Context, tableNameMapping map[string]string, cdcFlowName string) *SetupFlowExecution {
	return &SetupFlowExecution{
		tableNameMapping: tableNameMapping,
		cdcFlowName:      cdcFlowName,
		executionID:      workflow.GetInfo(ctx).WorkflowExecution.ID,
		logger:           workflow.GetLogger(ctx),
	}
}

// checkConnectionsAndSetupMetadataTables checks the connections to the source and destination peers
// and ensures that the metadata tables are setup.
func (s *SetupFlowExecution) checkConnectionsAndSetupMetadataTables(
	ctx workflow.Context,
	config *protos.FlowConnectionConfigs,
) error {
	s.logger.Info("checking connections for CDC flow - ", s.cdcFlowName)

	checkCtx := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
		StartToCloseTimeout: time.Minute,
	})

	// first check the source peer connection
	srcConnStatusFuture := workflow.ExecuteLocalActivity(checkCtx, flowable.CheckConnection, &protos.SetupInput{
		Peer:     config.Source,
		FlowName: config.FlowJobName,
	})
	var srcConnStatus activities.CheckConnectionResult
	if err := srcConnStatusFuture.Get(checkCtx, &srcConnStatus); err != nil {
		return fmt.Errorf("failed to check source peer connection: %w", err)
	}

	dstSetupInput := &protos.SetupInput{
		Peer:     config.Destination,
		FlowName: config.FlowJobName,
	}

	// then check the destination peer connection
	destConnStatusFuture := workflow.ExecuteLocalActivity(checkCtx, flowable.CheckConnection, dstSetupInput)
	var destConnStatus activities.CheckConnectionResult
	if err := destConnStatusFuture.Get(checkCtx, &destConnStatus); err != nil {
		return fmt.Errorf("failed to check destination peer connection: %w", err)
	}

	s.logger.Info("ensuring metadata table exists - ", s.cdcFlowName)

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
		s.logger.Info("destination peer metadata tables already exist")
	}

	return nil
}

// ensurePullability ensures that the source peer is pullable.
func (s *SetupFlowExecution) ensurePullability(
	ctx workflow.Context,
	config *protos.FlowConnectionConfigs,
	checkConstraints bool,
) (map[uint32]string, error) {
	s.logger.Info("ensuring pullability for peer flow - ", s.cdcFlowName)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 15 * time.Minute,
	})
	srcTableIdNameMapping := make(map[uint32]string)

	srcTblIdentifiers := maps.Keys(s.tableNameMapping)
	sort.Strings(srcTblIdentifiers)

	// create EnsurePullabilityInput for the srcTableName
	ensurePullabilityInput := &protos.EnsurePullabilityBatchInput{
		PeerConnectionConfig:   config.Source,
		FlowJobName:            s.cdcFlowName,
		SourceTableIdentifiers: srcTblIdentifiers,
		CheckConstraints:       checkConstraints,
	}

	future := workflow.ExecuteActivity(ctx, flowable.EnsurePullability, ensurePullabilityInput)
	var ensurePullabilityOutput *protos.EnsurePullabilityBatchOutput
	if err := future.Get(ctx, &ensurePullabilityOutput); err != nil {
		s.logger.Error("failed to ensure pullability for tables: ", err)
		return nil, fmt.Errorf("failed to ensure pullability for tables: %w", err)
	}

	sortedTableNames := maps.Keys(ensurePullabilityOutput.TableIdentifierMapping)
	sort.Strings(sortedTableNames)

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
	s.logger.Info("creating raw table on destination - ", s.cdcFlowName)
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
	})

	// attempt to create the tables.
	createRawTblInput := &protos.CreateRawTableInput{
		PeerConnectionConfig: config.Destination,
		FlowJobName:          s.cdcFlowName,
		TableNameMapping:     s.tableNameMapping,
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
	s.logger.Info("fetching table schema for peer flow - ", s.cdcFlowName)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 1 * time.Hour,
		HeartbeatTimeout:    time.Minute,
	})

	sourceTables := maps.Keys(s.tableNameMapping)
	sort.Strings(sourceTables)

	tableSchemaInput := &protos.GetTableSchemaBatchInput{
		PeerConnectionConfig: flowConnectionConfigs.Source,
		TableIdentifiers:     sourceTables,
		FlowName:             s.cdcFlowName,
	}

	future := workflow.ExecuteActivity(ctx, flowable.GetTableSchema, tableSchemaInput)

	var tblSchemaOutput *protos.GetTableSchemaBatchOutput
	if err := future.Get(ctx, &tblSchemaOutput); err != nil {
		s.logger.Error("failed to fetch schema for source tables: ", err)
		return nil, fmt.Errorf("failed to fetch schema for source table %s: %w", sourceTables, err)
	}

	tableNameSchemaMapping := tblSchemaOutput.TableNameSchemaMapping
	sortedSourceTables := maps.Keys(tableNameSchemaMapping)
	sort.Strings(sortedSourceTables)

	s.logger.Info("setting up normalized tables for peer flow - ", s.cdcFlowName)
	normalizedTableMapping := make(map[string]*protos.TableSchema)
	for _, srcTableName := range sortedSourceTables {
		tableSchema := tableNameSchemaMapping[srcTableName]
		normalizedTableName := s.tableNameMapping[srcTableName]
		for _, mapping := range flowConnectionConfigs.TableMappings {
			if mapping.SourceTableIdentifier == srcTableName {
				if len(mapping.Exclude) != 0 {
					columnCount := utils.TableSchemaColumns(tableSchema)
					columnNames := make([]string, 0, columnCount)
					columnTypes := make([]string, 0, columnCount)
					utils.IterColumns(tableSchema, func(columnName, columnType string) {
						if !slices.Contains(mapping.Exclude, columnName) {
							columnNames = append(columnNames, columnName)
							columnTypes = append(columnTypes, columnType)
						}
					})
					tableSchema = &protos.TableSchema{
						TableIdentifier:       tableSchema.TableIdentifier,
						PrimaryKeyColumns:     tableSchema.PrimaryKeyColumns,
						IsReplicaIdentityFull: tableSchema.IsReplicaIdentityFull,
						ColumnNames:           columnNames,
						ColumnTypes:           columnTypes,
					}
				}
				break
			}
		}
		normalizedTableMapping[normalizedTableName] = tableSchema

		s.logger.Info("normalized table schema: ", normalizedTableName, " -> ", tableSchema)
	}

	// now setup the normalized tables on the destination peer
	setupConfig := &protos.SetupNormalizedTableBatchInput{
		PeerConnectionConfig:   flowConnectionConfigs.Destination,
		TableNameSchemaMapping: normalizedTableMapping,
		SoftDeleteColName:      flowConnectionConfigs.SoftDeleteColName,
		SyncedAtColName:        flowConnectionConfigs.SyncedAtColName,
		FlowName:               flowConnectionConfigs.FlowJobName,
	}

	future = workflow.ExecuteActivity(ctx, flowable.CreateNormalizedTable, setupConfig)
	var createNormalizedTablesOutput *protos.SetupNormalizedTableBatchOutput
	if err := future.Get(ctx, &createNormalizedTablesOutput); err != nil {
		s.logger.Error("failed to create normalized tables: ", err)
		return nil, fmt.Errorf("failed to create normalized tables: %w", err)
	}

	s.logger.Info("finished setting up normalized tables for peer flow - ", s.cdcFlowName)
	return normalizedTableMapping, nil
}

// executeSetupFlow executes the setup flow.
func (s *SetupFlowExecution) executeSetupFlow(
	ctx workflow.Context,
	config *protos.FlowConnectionConfigs,
) (*protos.SetupFlowOutput, error) {
	s.logger.Info("executing setup flow - ", s.cdcFlowName)

	// first check the connectionsAndSetupMetadataTables
	if err := s.checkConnectionsAndSetupMetadataTables(ctx, config); err != nil {
		return nil, fmt.Errorf("failed to check connections and setup metadata tables: %w", err)
	}

	setupFlowOutput := protos.SetupFlowOutput{}
	srcTableIdNameMapping, err := s.ensurePullability(ctx, config, !config.InitialSnapshotOnly)
	if err != nil {
		return nil, fmt.Errorf("failed to ensure pullability: %w", err)
	}
	setupFlowOutput.SrcTableIdNameMapping = srcTableIdNameMapping

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
	setupFlowOutput.TableNameSchemaMapping = tableNameSchemaMapping

	return &setupFlowOutput, nil
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
