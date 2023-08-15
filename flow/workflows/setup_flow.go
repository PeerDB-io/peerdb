package peerflow

import (
	"fmt"
	"time"

	"github.com/PeerDB-io/peer-flow/activities"
	"github.com/PeerDB-io/peer-flow/concurrency"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	cmap "github.com/orcaman/concurrent-map/v2"

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

type SetupFlowState struct {
	PeerFlowName string
	Progress     []string
}

type SetupFlowExecution struct {
	SetupFlowState
	executionID string
	logger      log.Logger
}

// NewSetupFlowExecution creates a new instance of SetupFlowExecution.
func NewSetupFlowExecution(ctx workflow.Context, state *SetupFlowState) *SetupFlowExecution {
	return &SetupFlowExecution{
		SetupFlowState: *state,
		executionID:    workflow.GetInfo(ctx).WorkflowExecution.ID,
		logger:         workflow.GetLogger(ctx),
	}
}

// checkConnectionsAndSetupMetadataTables checks the connections to the source and destination peers
// and ensures that the metadata tables are setup.
func (s *SetupFlowExecution) checkConnectionsAndSetupMetadataTables(
	ctx workflow.Context,
	config *protos.FlowConnectionConfigs,
) error {
	s.logger.Info("checking connections for peer flow - ", s.PeerFlowName)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
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

	s.logger.Info("ensuring metadata table exists - ", s.PeerFlowName)

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
func (s *SetupFlowExecution) ensurePullability(
	ctx workflow.Context,
	config *protos.FlowConnectionConfigs,
) error {
	s.logger.Info("ensuring pullability for peer flow - ", s.PeerFlowName)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
	})
	tmpMap := make(map[uint32]string)

	boundSelector := concurrency.NewBoundSelector(8, ctx)

	for srcTableName := range config.TableNameMapping {
		source := srcTableName

		// create EnsurePullabilityInput for the srcTableName
		ensurePullabilityInput := &protos.EnsurePullabilityInput{
			PeerConnectionConfig:  config.Source,
			FlowJobName:           s.PeerFlowName,
			SourceTableIdentifier: source,
		}

		future := workflow.ExecuteActivity(ctx, flowable.EnsurePullability, ensurePullabilityInput)
		boundSelector.AddFuture(future, func(f workflow.Future) error {
			var ensurePullabilityOutput protos.EnsurePullabilityOutput
			if err := f.Get(ctx, &ensurePullabilityOutput); err != nil {
				s.logger.Error("failed to ensure pullability: ", err)
				return err
			}

			switch typedEnsurePullabilityOutput := ensurePullabilityOutput.TableIdentifier.TableIdentifier.(type) {
			case *protos.TableIdentifier_PostgresTableIdentifier:
				tmpMap[typedEnsurePullabilityOutput.PostgresTableIdentifier.RelId] = source
			}

			return nil
		})
	}

	if err := boundSelector.Wait(); err != nil {
		return fmt.Errorf("failed to ensure pullability: %w", err)
	}

	config.SrcTableIdNameMapping = tmpMap
	return nil
}

// createRawTable creates the raw table on the destination peer.
func (s *SetupFlowExecution) createRawTable(
	ctx workflow.Context,
	config *protos.FlowConnectionConfigs,
) error {
	s.logger.Info("creating raw table on destination - ", s.PeerFlowName)
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
	})

	// attempt to create the tables.
	createRawTblInput := &protos.CreateRawTableInput{
		PeerConnectionConfig: config.Destination,
		FlowJobName:          s.PeerFlowName,
		TableNameMapping:     config.TableNameMapping,
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
	ctx workflow.Context, flowConnectionConfigs *protos.FlowConnectionConfigs) (map[string]*protos.TableSchema, error) {
	s.logger.Info("fetching table schema for peer flow - ", s.PeerFlowName)

	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
	})

	tableNameSchemaMapping := cmap.New[*protos.TableSchema]()

	boundSelector := concurrency.NewBoundSelector(8, ctx)

	for srcTableName := range flowConnectionConfigs.TableNameMapping {
		source := srcTableName

		// fetch source table schema for the normalized table setup.
		tableSchemaInput := &protos.GetTableSchemaInput{
			PeerConnectionConfig: flowConnectionConfigs.Source,
			TableIdentifier:      source,
		}

		future := workflow.ExecuteActivity(ctx, flowable.GetTableSchema, tableSchemaInput)
		boundSelector.AddFuture(future, func(f workflow.Future) error {
			s.logger.Info("fetching schema for source table - ", source)
			var srcTableSchema *protos.TableSchema
			if err := f.Get(ctx, &srcTableSchema); err != nil {
				s.logger.Error("failed to fetch schema for source table: ", err)
				return err
			}

			dstTableName, ok := flowConnectionConfigs.TableNameMapping[source]
			if !ok {
				s.logger.Error("failed to find destination table name for source table: ", source)
				return fmt.Errorf("failed to find destination table name for source table %s", source)
			}

			tableNameSchemaMapping.Set(dstTableName, srcTableSchema)
			return nil
		})
	}

	if err := boundSelector.Wait(); err != nil {
		s.logger.Error("failed to fetch table schema: ", err)
		return nil, fmt.Errorf("failed to fetch table schema: %w", err)
	}

	s.logger.Info("setting up normalized tables for peer flow - ",
		s.PeerFlowName, flowConnectionConfigs.TableNameMapping, tableNameSchemaMapping)

	// now setup the normalized tables on the destination peer
	setupConfig := &protos.SetupNormalizedTableParallelInput{
		PeerConnectionConfig:   flowConnectionConfigs.Destination,
		TableNameSchemaMapping: flowConnectionConfigs.TableNameSchemaMapping,
	}

	future := workflow.ExecuteActivity(ctx, flowable.CreateNormalizedTable, setupConfig)
	var createNormalizedTablesOutput protos.SetupNormalizedTableParallelOutput
	if err := future.Get(ctx, &createNormalizedTablesOutput); err != nil {
		s.logger.Error("failed to create normalized tables: ", err)
		return nil, fmt.Errorf("failed to create normalized tables: ", err)
	}

	s.logger.Info("finished setting up normalized tables for peer flow - ", s.PeerFlowName)
	return tableNameSchemaMapping.Items(), nil
}

// executeSetupFlow executes the setup flow.
func (s *SetupFlowExecution) executeSetupFlow(
	ctx workflow.Context,
	config *protos.FlowConnectionConfigs,
) (map[string]*protos.TableSchema, error) {
	s.logger.Info("executing setup flow - ", s.PeerFlowName)

	// first check the connectionsAndSetupMetadataTables
	if err := s.checkConnectionsAndSetupMetadataTables(ctx, config); err != nil {
		return nil, fmt.Errorf("failed to check connections and setup metadata tables: %w", err)
	}

	// then ensure pullability
	if err := s.ensurePullability(ctx, config); err != nil {
		return nil, fmt.Errorf("failed to ensure pullability: %w", err)
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
	config *protos.FlowConnectionConfigs) (*protos.FlowConnectionConfigs, error) {
	setupFlowState := &SetupFlowState{
		PeerFlowName: config.FlowJobName,
		Progress:     []string{},
	}

	// create the setup flow execution
	setupFlowExecution := NewSetupFlowExecution(ctx, setupFlowState)

	// execute the setup flow
	tableNameSchemaMapping, err := setupFlowExecution.executeSetupFlow(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to execute setup flow: %w", err)
	}
	config.TableNameSchemaMapping = tableNameSchemaMapping

	return config, nil
}
