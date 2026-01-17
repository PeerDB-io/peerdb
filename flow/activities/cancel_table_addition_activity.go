package activities

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	tEnums "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peerdb/flow/alerting"
	"github.com/PeerDB-io/peerdb/flow/connectors"
	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/shared"
	"github.com/PeerDB-io/peerdb/flow/workflows/cdc_state"
)

type CancelTableAdditionActivity struct {
	CatalogPool    shared.CatalogPool
	Alerter        *alerting.Alerter
	TemporalClient client.Client
	OtelManager    *otel_metrics.OtelManager
}

/*
GetCompletedTablesFromQrepRuns gets the list of tables in the addition request
whose snapshot has completed
*/
func (a *CancelTableAdditionActivity) GetCompletedTablesFromQrepRuns(
	ctx context.Context,
	flowJobName string,
	workflowId string,
) ([]string, error) {
	shutdown := common.HeartbeatRoutine(ctx, func() string {
		return "fetching completed tables from qrep_runs"
	})
	defer shutdown()

	peerflowRunId, err := a.getRunIDOfLatestRunningPeerFlow(ctx, workflowId)
	if err != nil {
		return nil, fmt.Errorf("failed to get run ID of latest running peer flow for workflow %s: %w", workflowId, err)
	}

	slog.InfoContext(ctx, "Fetching completed tables from qrep_runs for table addition cancellation",
		slog.String("flowName", flowJobName),
		slog.String("peerflowRunId", peerflowRunId))

	listResp, err := a.TemporalClient.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Query: fmt.Sprintf("RootRunId = '%s' AND WorkflowType = 'QRepFlowWorkflow'", peerflowRunId),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list qrep workflows for flow %s: %w", flowJobName, err)
	}

	if len(listResp.Executions) == 0 {
		slog.InfoContext(ctx, "No QRep workflows found for flow, returning empty completed tables list",
			slog.String("flowName", flowJobName))
		return []string{}, nil
	}

	runIds := make([]string, 0, len(listResp.Executions))
	for _, execution := range listResp.Executions {
		runIds = append(runIds, execution.Execution.RunId)
	}

	rows, err := a.CatalogPool.Query(ctx, `
    SELECT DISTINCT source_table
    FROM peerdb_stats.qrep_runs
    WHERE parent_mirror_name = $1
    AND run_uuid = ANY($2)
    AND consolidate_complete = true`, flowJobName, runIds)
	if err != nil {
		return nil, err
	}

	completedTables, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (string, error) {
		var tableName string
		err := row.Scan(&tableName)
		return tableName, err
	})
	if err != nil {
		return nil, err
	}

	return completedTables, nil
}

func (a *CancelTableAdditionActivity) GetTableOIDsFromCatalog(
	ctx context.Context,
	flowJobName string,
	tableMappings []*protos.TableMapping,
) (map[uint32]string, error) {
	if len(tableMappings) == 0 {
		return nil, fmt.Errorf("no table mappings provided for GetTableOIDsFromCatalog for flow %s", flowJobName)
	}

	shutdown := common.HeartbeatRoutine(ctx, func() string {
		return "fetching table OIDs from catalog"
	})
	defer shutdown()

	// Extract destination table names from table mappings
	destinationTableNames := make([]string, 0, len(tableMappings))
	for _, tm := range tableMappings {
		destinationTableNames = append(destinationTableNames, tm.DestinationTableIdentifier)
	}

	// Load table schemas from catalog using the internal function
	tableSchemas, err := internal.LoadTableSchemasFromCatalog(ctx, a.CatalogPool, flowJobName, destinationTableNames)
	if err != nil {
		return nil, fmt.Errorf("failed to load table schemas from catalog for OID fetch: %w", err)
	}

	// Extract table OIDs from schemas, mapping OID to source table identifier
	tableOIDs := make(map[uint32]string)
	for _, tm := range tableMappings {
		schema, exists := tableSchemas[tm.DestinationTableIdentifier]
		if !exists {
			return nil, fmt.Errorf("table schema not found in catalog for table %s in flow %s", tm.DestinationTableIdentifier, flowJobName)
		}
		tableOIDs[schema.TableOid] = tm.SourceTableIdentifier
	}

	return tableOIDs, nil
}

func (a *CancelTableAdditionActivity) CleanupIncompleteTablesInStats(
	ctx context.Context,
	flowJobName string,
	completedTables []*protos.TableMapping,
) error {
	shutdown := common.HeartbeatRoutine(ctx, func() string {
		return "cleaning up qrep stats for incomplete tables"
	})
	defer shutdown()

	completedSourceTables := make([]string, 0, len(completedTables))
	completedDestinationTables := make([]string, 0, len(completedTables))
	for _, tm := range completedTables {
		completedSourceTables = append(completedSourceTables, tm.SourceTableIdentifier)
		completedDestinationTables = append(completedDestinationTables, tm.DestinationTableIdentifier)
	}
	// Remove partitions that belong to incomplete runs for incomplete tables
	// Since qrep_partitions doesn't have source_table, we filter via the qrep_runs join
	_, err := a.CatalogPool.Exec(ctx, `
		DELETE FROM peerdb_stats.qrep_partitions qp
		WHERE EXISTS (
			SELECT 1 FROM peerdb_stats.qrep_runs qr
			WHERE qr.flow_name = qp.flow_name
			AND qr.run_uuid = qp.run_uuid
			AND qr.parent_mirror_name = $1
			AND qr.consolidate_complete = false
			AND qr.source_table NOT IN (SELECT unnest($2::text[]))
		)`, flowJobName, completedSourceTables)
	if err != nil {
		return fmt.Errorf("failed to cleanup incomplete tables in qrep_partitions for flow %s: %w", flowJobName, err)
	}

	_, err = a.CatalogPool.Exec(ctx, `
		DELETE FROM peerdb_stats.qrep_runs
		WHERE parent_mirror_name = $1 AND consolidate_complete = false
		AND source_table NOT IN (SELECT unnest($2::text[]))`, flowJobName, completedSourceTables)
	if err != nil {
		return fmt.Errorf("failed to cleanup incomplete tables in qrep_runs for flow %s: %w", flowJobName, err)
	}

	// delete from table_schema_mapping
	_, err = a.CatalogPool.Exec(ctx, `
		DELETE FROM table_schema_mapping
		WHERE flow_name = $1 AND table_name NOT IN (SELECT unnest($2::text[]))`, flowJobName, completedDestinationTables)
	if err != nil {
		return fmt.Errorf("failed to cleanup incomplete tables in table_schema_mapping for flow %s: %w", flowJobName, err)
	}

	return nil
}

func (a *CancelTableAdditionActivity) GetFlowInfoFromCatalog(
	ctx context.Context,
	flowJobName string,
) (*protos.GetFlowInfoToCancelFromCatalogOutput, error) {
	var configBytes []byte
	var workflowID string
	var sourcePeerType protos.DBType
	err := a.CatalogPool.QueryRow(ctx, `
		SELECT workflow_id,
			(select type from peers where peers.id = flows.source_peer) as source_peer_type,
			config_proto
		FROM flows
		WHERE name = $1`,
		flowJobName).Scan(&workflowID, &sourcePeerType, &configBytes)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("flow job %s not found", flowJobName)
		}
		return nil, fmt.Errorf("unable to get flow config for flow %s: %w", flowJobName, err)
	}

	var config protos.FlowConnectionConfigsCore
	if err := proto.Unmarshal(configBytes, &config); err != nil {
		return nil, fmt.Errorf("unable to unmarshal flow config for flow %s: %w", flowJobName, err)
	}

	return &protos.GetFlowInfoToCancelFromCatalogOutput{
		FlowConnectionConfigs: &config,
		WorkflowId:            workflowID,
		SourcePeerType:        sourcePeerType,
	}, nil
}

func (a *CancelTableAdditionActivity) UpdateCdcJobEntry(
	ctx context.Context,
	connectionConfigs *protos.FlowConnectionConfigsCore,
	workflowID string,
) error {
	cfgBytes, err := proto.Marshal(connectionConfigs)
	if err != nil {
		return fmt.Errorf("unable to marshal flow config: %w", err)
	}

	if _, err = a.CatalogPool.Exec(ctx,
		`UPDATE flows
		SET status = $1, config_proto = $2
		WHERE name = $3`,
		protos.FlowStatus_STATUS_RUNNING, cfgBytes, connectionConfigs.FlowJobName,
	); err != nil {
		return fmt.Errorf("unable to update flows table for flow %s: %w",
			connectionConfigs.FlowJobName, err)
	}

	slog.InfoContext(ctx, "Successfully updated CDC job entry in catalog",
		slog.String("flowName", connectionConfigs.FlowJobName),
		slog.String("workflowID", workflowID))

	return nil
}

func (a *CancelTableAdditionActivity) CleanupCurrentParentMirror(ctx context.Context, flowJobName string, workflowId string) error {
	shutdown := common.HeartbeatRoutine(ctx, func() string {
		return "cleaning up current parent mirror and flow from catalog"
	})
	defer shutdown()

	// Describe to get latest run
	rootRunId, err := a.getRunIDOfLatestRunningPeerFlow(ctx, workflowId)
	if err != nil {
		return fmt.Errorf("failed to get run ID of latest running peer flow for workflow %s: %w", workflowId, err)
	}

	// Terminate the parent workflow
	err = a.TemporalClient.TerminateWorkflow(ctx, workflowId, "", "Canceling due to table addition cancellation")
	if err != nil {
		var notFoundErr *serviceerror.NotFound
		if errors.As(err, &notFoundErr) {
			slog.InfoContext(ctx, "Workflow already not found during cleanup of current parent mirror",
				slog.String("flowName", flowJobName),
				slog.String("workflowId", workflowId))
		} else {
			return fmt.Errorf("failed to terminate parent workflow %s: %w", workflowId, err)
		}
	} else {
		slog.InfoContext(ctx, "Successfully terminated parent workflow",
			slog.String("flowName", flowJobName),
			slog.String("workflowId", workflowId))
	}

	// Terminate all child workflows just to be sure
	listResp, err := a.TemporalClient.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Query: fmt.Sprintf("RootRunId = '%s'", rootRunId),
	})
	if err != nil {
		slog.WarnContext(ctx, "Failed to list child workflows during cleanup, continuing",
			slog.String("flowName", flowJobName),
			slog.String("workflowId", workflowId),
			slog.String("error", err.Error()))
	} else {
		for _, execution := range listResp.Executions {
			if execution.Execution.WorkflowId == workflowId {
				// Skip the root workflow itself
				continue
			}

			childErr := a.TemporalClient.TerminateWorkflow(ctx,
				execution.Execution.WorkflowId,
				execution.Execution.RunId,
				"Canceling child workflow due to table addition cancellation")
			if childErr != nil {
				var notFoundErr *serviceerror.NotFound
				if errors.As(childErr, &notFoundErr) {
					slog.InfoContext(ctx, "Child workflow already not found during cleanup",
						slog.String("flowName", flowJobName),
						slog.String("childWorkflowId", execution.Execution.WorkflowId))
				} else {
					slog.WarnContext(ctx, "Failed to terminate child workflow, continuing",
						slog.String("flowName", flowJobName),
						slog.String("childWorkflowId", execution.Execution.WorkflowId),
						slog.String("error", childErr.Error()))
				}
			} else {
				slog.InfoContext(ctx, "Successfully terminated child workflow",
					slog.String("flowName", flowJobName),
					slog.String("childWorkflowId", execution.Execution.WorkflowId))
			}
		}
	}

	slog.InfoContext(ctx, "Successfully cleaned up current parent mirror and flow from catalog",
		slog.String("flowName", flowJobName))
	return nil
}

func (a *CancelTableAdditionActivity) WaitForNewRunningMirrorToBeInRunningState(
	ctx context.Context,
	flowJobName string,
	workflowId string,
) error {
	flowStatus, err := internal.GetWorkflowStatus(ctx, a.CatalogPool, workflowId)
	if err != nil {
		return fmt.Errorf("failed to get workflow status for flow %s: %w", flowJobName, err)
	}

	if flowStatus != protos.FlowStatus_STATUS_RUNNING {
		return fmt.Errorf("expected flow %s to be in RUNNING state, but found %s", flowJobName, flowStatus.String())
	}

	slog.InfoContext(ctx, "New mirror flow is in RUNNING state", slog.String("flowName", flowJobName))
	return nil
}

func (a *CancelTableAdditionActivity) RemoveCancelledTablesFromPublicationIfApplicable(
	ctx context.Context,
	flowJobName string,
	sourcePeerName string,
	publicationNameInConfig string,
	finalListOfTables []*protos.TableMapping,
) error {
	shutdown := common.HeartbeatRoutine(ctx, func() string {
		return "removing cancelled tables from publication"
	})
	defer shutdown()
	peerType, err := connectors.LoadPeerType(ctx, a.CatalogPool, sourcePeerName)
	if err != nil {
		return fmt.Errorf("failed to load peer type for peer %s: %w", sourcePeerName, err)
	}

	if peerType != protos.DBType_POSTGRES {
		slog.InfoContext(ctx, "Source peer is not Postgres, skipping publication table removal",
			slog.String("flowName", flowJobName),
			slog.String("sourcePeerName", sourcePeerName),
			slog.String("peerType", peerType.String()))

		return nil
	}

	if publicationNameInConfig == "" {
		publicationName := connpostgres.GetDefaultPublicationName(flowJobName)
		slog.InfoContext(ctx, "Publication name not set in config, using default",
			slog.String("flowName", flowJobName),
			slog.String("publicationName", publicationName))

		conn, connClose, err := connectors.GetByNameAs[*connpostgres.PostgresConnector](ctx, nil, a.CatalogPool, sourcePeerName)
		if err != nil {
			return fmt.Errorf("failed to get connector for peer %s: %w", sourcePeerName, err)
		}
		defer connClose(ctx)

		var schemaQualifiedTables []*common.QualifiedTable
		for _, tm := range finalListOfTables {
			schemaTable, err := common.ParseTableIdentifier(tm.SourceTableIdentifier)
			if err != nil {
				return fmt.Errorf("error parsing table identifier %s: %w", tm.SourceTableIdentifier, err)
			}
			schemaQualifiedTables = append(schemaQualifiedTables, schemaTable)
		}
		schemaQualifiedTablesInPublication, err := conn.GetTablesFromPublication(ctx, publicationName, schemaQualifiedTables)
		if err != nil {
			return fmt.Errorf("failed to get tables from publication %s: %w", publicationName, err)
		}

		var publicationTablesMapping []*protos.TableMapping
		for _, schemaQualifiedTable := range schemaQualifiedTablesInPublication {
			publicationTablesMapping = append(publicationTablesMapping, &protos.TableMapping{
				SourceTableIdentifier: schemaQualifiedTable.Namespace + "." + schemaQualifiedTable.Table,
			})
		}
		err = conn.RemoveTablesFromPublication(ctx, &protos.RemoveTablesFromPublicationInput{
			FlowJobName:    flowJobName,
			TablesToRemove: publicationTablesMapping,
		})
		if err != nil {
			return fmt.Errorf("failed to remove tables from publication %s: %w", publicationName, err)
		}

		slog.InfoContext(ctx, "Successfully removed cancelled tables from publication",
			slog.String("flowName", flowJobName),
			slog.String("publicationName", publicationName))

		return nil
	} else {
		// skip because it is user-provided publication which we do not touch.
		slog.InfoContext(ctx, "Publication name set in config, skipping publication table removal",
			slog.String("flowName", flowJobName),
			slog.String("publicationName", publicationNameInConfig))

		return nil
	}
}

func (a *CancelTableAdditionActivity) StartNewCDCFlow(
	ctx context.Context,
	flowConfig *protos.FlowConnectionConfigsCore,
	state *cdc_state.CDCFlowWorkflowState,
	workflowID string,
) error {
	shutdown := common.HeartbeatRoutine(ctx, func() string {
		return "creating job entry and starting new CDC flow"
	})
	defer shutdown()

	workflowOptions := client.StartWorkflowOptions{
		ID:                       workflowID,
		TaskQueue:                string(shared.PeerFlowTaskQueue),
		TypedSearchAttributes:    shared.NewSearchAttributes(flowConfig.FlowJobName),
		WorkflowIDConflictPolicy: tEnums.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING, // idempotent behavior
		WorkflowIDReusePolicy:    tEnums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE, // allow reuse for retries
	}

	run, err := a.TemporalClient.ExecuteWorkflow(ctx, workflowOptions, "CDCFlowWorkflow", flowConfig, state)
	if err != nil {
		return fmt.Errorf("failed to start CDC workflow: %w", err)
	}

	slog.InfoContext(ctx, "Successfully started new CDC workflow",
		slog.String("flowName", flowConfig.FlowJobName),
		slog.String("workflowID", workflowID),
		slog.String("runID", run.GetRunID()))

	return nil
}

func (a *CancelTableAdditionActivity) getRunIDOfLatestRunningPeerFlow(ctx context.Context, workflowId string) (string, error) {
	// Describe to get latest run
	describeResp, err := a.TemporalClient.DescribeWorkflowExecution(ctx, workflowId, "")
	if err != nil {
		var notFoundErr *serviceerror.NotFound
		if errors.As(err, &notFoundErr) {
			return "", fmt.Errorf("workflow %s not found: %w", workflowId, err)
		} else {
			return "", fmt.Errorf("failed to describe workflow %s: %w", workflowId, err)
		}
	}
	if describeResp == nil || describeResp.WorkflowExecutionInfo == nil || describeResp.WorkflowExecutionInfo.Execution == nil {
		return "", fmt.Errorf("invalid describe response for workflow %s", workflowId)
	}
	return describeResp.WorkflowExecutionInfo.Execution.RunId, nil
}
