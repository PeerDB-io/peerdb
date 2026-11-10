package activities

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"

	"github.com/PeerDB-io/peerdb/flow/connectors"
	connclickhouse "github.com/PeerDB-io/peerdb/flow/connectors/clickhouse"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/model"
)

// TableResyncWorkflowName is registered by the workflows package, which imports this one.
const TableResyncWorkflowName = "TableResyncWorkflow"

// requestTableResyncs starts a TableResyncWorkflow for tables whose schema change cannot be replayed in place.
// One runs per mirror at a time; a table flagged while one runs is flagged again by its next relation message.
func (a *FlowableActivity) requestTableResyncs(
	ctx context.Context,
	config *protos.FlowConnectionConfigsCore,
	tableMappings []*protos.TableMapping,
	schemaDeltas []*protos.TableSchemaDelta,
	dstType protos.DBType,
) error {
	logger := internal.LoggerFromCtx(ctx)
	update := &protos.CDCFlowConfigUpdate{}
	for _, delta := range schemaDeltas {
		if !delta.ResyncRequired {
			continue
		}
		idx := slices.IndexFunc(tableMappings, func(tm *protos.TableMapping) bool {
			return tm.DestinationTableIdentifier == delta.DstTableName
		})
		if idx == -1 || dstType != protos.DBType_CLICKHOUSE {
			a.Alerter.LogFlowWarning(ctx, config.FlowJobName, fmt.Errorf(
				"table %s needs a manual resync: %s", delta.SrcTableName, delta.ResyncReason))
			continue
		}
		if !slices.ContainsFunc(update.AdditionalTables, func(tm *protos.TableMapping) bool {
			return tm.DestinationTableIdentifier == delta.DstTableName
		}) {
			update.AdditionalTables = append(update.AdditionalTables, tableMappings[idx])
			a.Alerter.LogFlowInfo(ctx, config.FlowJobName,
				fmt.Sprintf("resyncing table %s: %s", delta.SrcTableName, delta.ResyncReason))
		}
	}
	if len(update.AdditionalTables) == 0 {
		return nil
	}
	run, err := a.TemporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        "table-resync-" + config.FlowJobName,
		TaskQueue: activity.GetInfo(ctx).TaskQueue,
	}, TableResyncWorkflowName, config.FlowJobName, config.DestinationName, config.Env, update)
	if err != nil {
		return fmt.Errorf("failed to start table resync: %w", err)
	}
	logger.Info("table resync workflow running", slog.String("workflowID", run.GetID()), slog.String("runID", run.GetRunID()))
	return nil
}

// ResyncTables re-snapshots tables of a CDC mirror into ClickHouse: remove them from the mirror, drop the
// destination tables, add them back. Each step checks the mirror's state first, so a retry picks up where
// a failed attempt stopped.
func (a *FlowableActivity) ResyncTables(
	ctx context.Context,
	flowName string,
	dstPeer string,
	env map[string]string,
	update *protos.CDCFlowConfigUpdate,
) error {
	logger := internal.LoggerFromCtx(ctx)
	tables := update.AdditionalTables
	var workflowID string
	if err := a.CatalogPool.QueryRow(ctx, "SELECT workflow_id FROM flows WHERE name = $1", flowName).Scan(&workflowID); err != nil {
		return fmt.Errorf("failed to find workflow of %s: %w", flowName, err)
	}

	waitFor := func(target protos.FlowStatus) error {
		for {
			status, err := internal.GetWorkflowStatusByName(ctx, a.CatalogPool, flowName)
			if err != nil {
				return err
			}
			if status == target {
				return nil
			}
			switch status {
			case protos.FlowStatus_STATUS_FAILED, protos.FlowStatus_STATUS_TERMINATED, protos.FlowStatus_STATUS_TERMINATING,
				protos.FlowStatus_STATUS_COMPLETED:
				return fmt.Errorf("mirror %s is %s, stopping table resync", flowName, status)
			}
			activity.RecordHeartbeat(ctx, "waiting for "+target.String())
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(10 * time.Second):
			}
		}
	}
	pause := func() error {
		status, err := internal.GetWorkflowStatusByName(ctx, a.CatalogPool, flowName)
		if err != nil {
			return err
		}
		if status == protos.FlowStatus_STATUS_RUNNING {
			if err := model.FlowSignal.SignalClientWorkflow(ctx, a.TemporalClient, workflowID, "", model.PauseSignal); err != nil {
				return fmt.Errorf("failed to pause %s: %w", flowName, err)
			}
		}
		return waitFor(protos.FlowStatus_STATUS_PAUSED)
	}
	// the same signals the state change API sends: the config update, then resume
	applyAndResume := func(change *protos.CDCFlowConfigUpdate) error {
		if err := pause(); err != nil {
			return err
		}
		if err := model.CDCDynamicPropertiesSignal.SignalClientWorkflow(ctx, a.TemporalClient, workflowID, "", change); err != nil {
			return fmt.Errorf("failed to update tables of %s: %w", flowName, err)
		}
		if err := model.FlowSignal.SignalClientWorkflow(ctx, a.TemporalClient, workflowID, "", model.NoopSignal); err != nil {
			return fmt.Errorf("failed to resume %s: %w", flowName, err)
		}
		return waitFor(protos.FlowStatus_STATUS_RUNNING)
	}
	inMirror := func() ([]*protos.TableMapping, error) {
		cfg, err := internal.FetchConfigFromDB(ctx, a.CatalogPool, flowName)
		if err != nil {
			return nil, err
		}
		var present []*protos.TableMapping
		for _, tm := range tables {
			if slices.ContainsFunc(cfg.TableMappings, func(m *protos.TableMapping) bool {
				return m.DestinationTableIdentifier == tm.DestinationTableIdentifier
			}) {
				present = append(present, tm)
			}
		}
		return present, nil
	}

	present, err := inMirror()
	if err != nil {
		return err
	}
	if len(present) > 0 {
		logger.Info("table resync: removing tables", slog.Int("tables", len(present)))
		if err := applyAndResume(&protos.CDCFlowConfigUpdate{RemovedTables: present}); err != nil {
			return err
		}
	}

	dstConn, dstClose, err := connectors.GetByNameAs[*connclickhouse.ClickHouseConnector](ctx, env, a.CatalogPool, dstPeer)
	if err != nil {
		return fmt.Errorf("failed to connect to destination %s: %w", dstPeer, err)
	}
	defer dstClose(ctx)
	for _, tm := range tables {
		if err := dstConn.DropTableForResync(ctx, tm.DestinationTableIdentifier); err != nil {
			return err
		}
	}

	logger.Info("table resync: adding tables back", slog.Int("tables", len(tables)))
	if err := applyAndResume(&protos.CDCFlowConfigUpdate{AdditionalTables: tables}); err != nil {
		return err
	}
	a.Alerter.LogFlowInfo(ctx, flowName, fmt.Sprintf("table resync done for %d table(s)", len(tables)))
	return nil
}
