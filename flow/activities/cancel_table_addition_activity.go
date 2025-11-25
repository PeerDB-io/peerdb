package activities

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peerdb/flow/alerting"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type CancelTableAdditionActivity struct {
	CatalogPool    shared.CatalogPool
	Alerter        *alerting.Alerter
	TemporalClient client.Client
	OtelManager    *otel_metrics.OtelManager
}

/* GetCompletedTablesInQrepRuns gets the list of source tables whose latest QRep run is completed */
func (a *CancelTableAdditionActivity) GetCompletedTablesInQrepRuns(ctx context.Context, flowJobName string) ([]string, error) {
	shutdown := heartbeatRoutine(ctx, func() string {
		return "fetching completed tables from qrep_runs"
	})
	defer shutdown()
	rows, err := a.CatalogPool.Query(ctx, `
		WITH latest_runs AS (
            SELECT 
                source_table,
                consolidate_complete,
                ROW_NUMBER() OVER (PARTITION BY source_table ORDER BY id DESC) as rn
            FROM peerdb_stats.qrep_runs 
            WHERE parent_mirror_name = $1
        )
        SELECT source_table 
        FROM latest_runs 
        WHERE rn = 1 AND consolidate_complete = true`, flowJobName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var completedTables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		completedTables = append(completedTables, tableName)
	}
	if err := rows.Err(); err != nil {
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

	shutdown := heartbeatRoutine(ctx, func() string {
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
	shutdown := heartbeatRoutine(ctx, func() string {
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

func (a *CancelTableAdditionActivity) GetFlowConfigFromCatalog(
	ctx context.Context,
	flowJobName string,
) (*protos.FlowConnectionConfigsCore, error) {
	var configBytes []byte
	err := a.CatalogPool.QueryRow(ctx,
		"SELECT config_proto FROM flows WHERE name = $1",
		flowJobName).Scan(&configBytes)
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

	return &config, nil
}

func (a *CancelTableAdditionActivity) CleanupCurrentParentMirror(ctx context.Context, flowJobName string) error {
	shutdown := heartbeatRoutine(ctx, func() string {
		return "cleaning up current parent mirror and flow from catalog"
	})
	defer shutdown()
	workflowID := shared.GetWorkflowID(flowJobName)
	err := a.TemporalClient.TerminateWorkflow(ctx, workflowID, "", "Canceling due to table addition cancellation")
	if err != nil {
		if err.Error() != "workflow execution already completed" {
			return fmt.Errorf("failed to cancel peerflow workflow %s: %w", workflowID, err)
		}
	}

	// terminate all child workflows
	listResp, err := a.TemporalClient.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Query: fmt.Sprintf("RootWorkflowId = '%s'", workflowID),
	})
	if err != nil {
		return fmt.Errorf("failed to list child workflows for %s: %w", workflowID, err)
	}

	for _, execution := range listResp.Executions {
		if execution.Execution.WorkflowId == workflowID {
			// skip root workflow
			continue
		}
		err := a.TemporalClient.TerminateWorkflow(
			ctx, execution.Execution.WorkflowId, execution.Execution.RunId,
			"Canceling child workflow due to table addition cancellation")
		if err != nil && err.Error() != "workflow execution already completed" {
			return fmt.Errorf("failed to cancel child workflow %s: %w", execution.Execution.WorkflowId, err)
		}
	}

	// delete from flows table in catalog
	_, err = a.CatalogPool.Exec(ctx, "DELETE FROM flows WHERE name = $1", flowJobName)
	if err != nil {
		return fmt.Errorf("failed to delete flow %s from catalog: %w", flowJobName, err)
	}

	internal.LoggerFromCtx(ctx).Info("Successfully cleaned up current parent mirror and flow from catalog",
		slog.String("flowName", flowJobName))
	return nil
}

func (a *CancelTableAdditionActivity) CreateCdcJobEntry(
	ctx context.Context,
	connectionConfigs *protos.FlowConnectionConfigsCore,
	workflowID string,
) error {
	// idempotent=false so that in an outlandish case when the flow entry is somehow there,
	//  we have a chance to manually clean it up because its configs are wrong
	err := shared.CreateCdcJobEntry(ctx, a.CatalogPool, connectionConfigs, workflowID, false)
	if err != nil {
		return err
	}

	internal.LoggerFromCtx(ctx).Info("Successfully created CDC job entry in catalog",
		slog.String("flowName", connectionConfigs.FlowJobName),
		slog.String("workflowID", workflowID))

	return nil
}

func (a *CancelTableAdditionActivity) WaitForNewRunningMirrorToBeInRunningState(
	ctx context.Context,
	flowJobName string,
) error {
	flowStatus, err := internal.GetWorkflowStatus(ctx, a.CatalogPool, shared.GetWorkflowID(flowJobName))
	if err != nil {
		return fmt.Errorf("failed to get workflow status for flow %s: %w", flowJobName, err)
	}

	if flowStatus != protos.FlowStatus_STATUS_RUNNING {
		return fmt.Errorf("expected flow %s to be in RUNNING state, but found %s", flowJobName, flowStatus.String())
	}

	internal.LoggerFromCtx(ctx).Info("New mirror flow is in RUNNING state", slog.String("flowName", flowJobName))
	return nil
}
