package peerflow

import (
	"fmt"
	"time"

	"github.com/PeerDB-io/peer-flow/activities"
	"github.com/PeerDB-io/peer-flow/generated/protos"

	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"
)

// SetupFlow is the workflow that is responsible for ensuring all the
// setup is done for a sync flow to execute.
//
// The setup flow is responsible for:
//   1. Global:
//     - ensure that we are able to connect to the source and destination peers
//   2. Source Peer:
// 	   - setup the metadata table on the source peer
//     - initialize pullability on the source peer, as an example on postgres:
//       - ensuring the required table exists on the source peer
//	     - creating the slot and publication on the source peer
//   3. Destination Peer:
// 	   - setup the metadata table on the destination peer
//     - creating the raw table on the destination peer
//     - creating the normalized table on the destination peer

type SetupFlowWorkflowInput struct {
	CatalogJdbcURL string
	FlowName       string
}

type SetupFlowWorkflowExecution struct {
	SetupFlowWorkflowInput
	executionID string
	logger      log.Logger
}

// newSetupFlowWorkflowExecution creates a new instance of SetupFlowExecution.
func newSetupFlowWorkflowExecution(ctx workflow.Context, config *SetupFlowWorkflowInput) *SetupFlowWorkflowExecution {
	return &SetupFlowWorkflowExecution{
		SetupFlowWorkflowInput: *config,
		executionID:            workflow.GetInfo(ctx).WorkflowExecution.ID,
		logger:                 workflow.GetLogger(ctx),
	}
}

// checkConnectionsAndSetupMetadataTables checks the connections to the source and destination peers
// and ensures that the metadata tables are setup.
func (s *SetupFlowWorkflowExecution) checkConnectionsAndSetupMetadataTables(
	ctx workflow.Context,
	config *protos.FlowConnectionConfigs,
) error {
	s.logger.Info("checking connections for peer flow - ", s.SetupFlowWorkflowInput.FlowName)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
	})

	// first check the source peer connection
	srcConnStatusFuture := workflow.ExecuteActivity(ctx, flowable.CheckConnection, config.Source)
	var srcConnStatus activities.CheckConnectionResult
	if err := srcConnStatusFuture.Get(ctx, &srcConnStatus); err != nil {
		return fmt.Errorf("failed to check source peer connection: %w", err)
	}

	// then check the destination peer connection
	destConnStatusFuture := workflow.ExecuteActivity(ctx, flowable.CheckConnection, config.Destination)
	var destConnStatus activities.CheckConnectionResult
	if err := destConnStatusFuture.Get(ctx, &destConnStatus); err != nil {
		return fmt.Errorf("failed to check destination peer connection: %w", err)
	}

	s.logger.Info("ensuring metadata table exists - ", s.SetupFlowWorkflowInput.FlowName)

	if srcConnStatus.NeedsSetupMetadataTables {
		fSrc := workflow.ExecuteActivity(ctx, flowable.SetupMetadataTables, config.Source)
		if err := fSrc.Get(ctx, nil); err != nil {
			return fmt.Errorf("failed to setup source peer metadata tables: %w", err)
		}
	} else {
		s.logger.Info("source peer metadata tables already exist")
	}

	// then setup the destination peer metadata tables
	if destConnStatus.NeedsSetupMetadataTables {
		fDst := workflow.ExecuteActivity(ctx, flowable.SetupMetadataTables, config.Destination)
		if err := fDst.Get(ctx, nil); err != nil {
			return fmt.Errorf("failed to setup destination peer metadata tables: %w", err)
		}
	} else {
		s.logger.Info("destination peer metadata tables already exist")
	}

	return nil
}

// ensurePullability ensures that the source peer is pullable.
func (s *SetupFlowWorkflowExecution) ensurePullability(
	ctx workflow.Context,
	config *protos.FlowConnectionConfigs,
) error {
	s.logger.Info("ensuring pullability for peer flow - ", s.SetupFlowWorkflowInput.FlowName)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
	})
	tmpMap := make(map[uint32]string)

	for srcTableName := range config.TableNameMapping {

		// create EnsurePullabilityInput for the srcTableName
		ensurePullabilityInput := &protos.EnsurePullabilityInput{
			PeerConnectionConfig:  config.Source,
			FlowJobName:           s.SetupFlowWorkflowInput.FlowName,
			SourceTableIdentifier: srcTableName,
		}

		// ensure pullability
		var ensurePullabilityOutput protos.EnsurePullabilityOutput
		ensurePullFuture := workflow.ExecuteActivity(ctx, flowable.EnsurePullability, ensurePullabilityInput)
		if err := ensurePullFuture.Get(ctx, &ensurePullabilityOutput); err != nil {
			return fmt.Errorf("failed to ensure pullability: %w", err)
		}

		switch typedEnsurePullabilityOutput := ensurePullabilityOutput.TableIdentifier.TableIdentifier.(type) {
		case *protos.TableIdentifier_PostgresTableIdentifier:
			tmpMap[typedEnsurePullabilityOutput.PostgresTableIdentifier.RelId] = srcTableName
		}

	}
	config.SrcTableIdNameMapping = tmpMap
	return nil
}

// ensurePullability ensures that the source peer is pullable.
func (s *SetupFlowWorkflowExecution) setupReplication(
	ctx workflow.Context,
	config *protos.FlowConnectionConfigs,
) error {
	s.logger.Info("setting up replication on source for peer flow - ", s.SetupFlowWorkflowInput.FlowName)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
	})
	setupReplicationInput := &protos.SetupReplicationInput{
		PeerConnectionConfig: config.Source,
		FlowJobName:          s.SetupFlowWorkflowInput.FlowName,
		TableNameMapping:     config.TableNameMapping,
	}
	setupReplicationFuture := workflow.ExecuteActivity(ctx, flowable.SetupReplication, setupReplicationInput)
	if err := setupReplicationFuture.Get(ctx, nil); err != nil {
		return fmt.Errorf("failed to setup replication on source peer: %w", err)
	}
	return nil
}

// createRawTable creates the raw table on the destination peer.
func (s *SetupFlowWorkflowExecution) createRawTable(
	ctx workflow.Context,
	config *protos.FlowConnectionConfigs,
) error {
	s.logger.Info("creating raw table on destination - ", s.SetupFlowWorkflowInput.FlowName)
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
	})

	// attempt to create the tables.
	createRawTblInput := &protos.CreateRawTableInput{
		PeerConnectionConfig: config.Destination,
		FlowJobName:          s.SetupFlowWorkflowInput.FlowName,
	}

	rawTblFuture := workflow.ExecuteActivity(ctx, flowable.CreateRawTable, createRawTblInput)
	if err := rawTblFuture.Get(ctx, nil); err != nil {
		return fmt.Errorf("failed to create raw table: %w", err)
	}

	return nil
}

// fetchTableSchemaAndSetupNormalizedTables fetches the table schema for the source table and
// sets up the normalized tables on the destination peer.
func (s *SetupFlowWorkflowExecution) fetchTableSchemaAndSetupNormalizedTables(
	ctx workflow.Context, flowConnectionConfigs *protos.FlowConnectionConfigs) (map[string]*protos.TableSchema, error) {
	s.logger.Info("fetching table schema for peer flow - ", s.SetupFlowWorkflowInput.FlowName)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
	})

	tableNameSchemaMapping := make(map[string]*protos.TableSchema)
	// fetch source table schema for the normalized table setup.
	for srcTableName := range flowConnectionConfigs.TableNameMapping {
		tableSchemaInput := &protos.GetTableSchemaInput{
			PeerConnectionConfig: flowConnectionConfigs.Source,
			TableIdentifier:      srcTableName,
		}
		fSrcTableSchema := workflow.ExecuteActivity(ctx, flowable.GetTableSchema, tableSchemaInput)
		var srcTableSchema *protos.TableSchema
		if err := fSrcTableSchema.Get(ctx, &srcTableSchema); err != nil {
			return nil, fmt.Errorf("failed to fetch schema for source table %s: %w", srcTableName, err)
		}
		s.logger.Info("fetched schema for table %s for peer flow %s ", srcTableSchema, s.SetupFlowWorkflowInput.FlowName)

		tableNameSchemaMapping[flowConnectionConfigs.TableNameMapping[srcTableName]] = srcTableSchema

		s.logger.Info("setting up normalized table for table %s for peer flow - ", srcTableSchema, s.SetupFlowWorkflowInput.FlowName)

		// now setup the normalized tables on the destination peer
		setupConfig := &protos.SetupNormalizedTableInput{
			PeerConnectionConfig: flowConnectionConfigs.Destination,
			TableIdentifier:      flowConnectionConfigs.TableNameMapping[srcTableName],
			SourceTableSchema:    srcTableSchema,
		}
		fSetupNormalizedTables := workflow.ExecuteActivity(ctx, flowable.CreateNormalizedTable, setupConfig)

		var setupOutput *protos.SetupNormalizedTableOutput
		if err := fSetupNormalizedTables.Get(ctx, &setupOutput); err != nil {
			return nil, fmt.Errorf("failed to setup normalized tables: %w", err)
		}
		s.logger.Info("set up normalized table for source table %s, dest table %s and peer flow %s",
			srcTableName, flowConnectionConfigs.TableNameMapping[srcTableName], s.SetupFlowWorkflowInput.FlowName)
	}

	// initialize the table schema on the destination peer

	return tableNameSchemaMapping, nil
}

// executeSetupFlow executes the setup flow.
func (s *SetupFlowWorkflowExecution) executeSetupFlow(
	ctx workflow.Context,
	config *protos.FlowConnectionConfigs,
) (map[string]*protos.TableSchema, error) {
	s.logger.Info("executing setup flow - ", s.SetupFlowWorkflowInput.FlowName)

	// first check the connectionsAndSetupMetadataTables
	if err := s.checkConnectionsAndSetupMetadataTables(ctx, config); err != nil {
		return nil, fmt.Errorf("failed to check connections and setup metadata tables: %w", err)
	}

	// then ensure pullability
	if err := s.ensurePullability(ctx, config); err != nil {
		return nil, fmt.Errorf("failed to ensure pullability: %w", err)
	}

	// then setup replication
	if err := s.setupReplication(ctx, config); err != nil {
		return nil, fmt.Errorf("failed to setup replication on source: %w", err)
	}

	// then create the raw table
	if err := s.createRawTable(ctx, config); err != nil {
		return nil, fmt.Errorf("failed to create raw table: %w", err)
	}

	// then fetch the table schema and setup the normalized tables
	tableSchema, err := s.fetchTableSchemaAndSetupNormalizedTables(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch table schema and setup normalized tables: %w", err)
	}

	return tableSchema, nil
}

// SetupFlowWorkflow is the workflow that sets up the flow.
func SetupFlowWorkflow(ctx workflow.Context,
	config *SetupFlowWorkflowInput) (*protos.FlowConnectionConfigs, error) {
	// create the setup flow execution
	setupFlowExecution := newSetupFlowWorkflowExecution(ctx, config)

	// fetch the connection configs
	fetchConnectionConfigsInput := FetchConnectionConfigInput{
		PeerFlowName:   config.FlowName,
		CatalogJdbcURL: config.CatalogJdbcURL,
		logger:         setupFlowExecution.logger,
	}
	connectionConfigs, err := fetchConnectionConfigs(ctx, &fetchConnectionConfigsInput)
	if err != nil {
		return nil, err
	}
	// execute the setup flow
	tableNameSchemaMapping, err := setupFlowExecution.executeSetupFlow(ctx, connectionConfigs)
	if err != nil {
		return nil, fmt.Errorf("failed to execute setup flow: %w", err)
	}
	connectionConfigs.TableNameSchemaMapping = tableNameSchemaMapping

	return connectionConfigs, nil
}
