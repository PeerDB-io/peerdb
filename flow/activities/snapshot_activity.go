package activities

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.temporal.io/sdk/activity"

	"github.com/PeerDB-io/peerdb/flow/alerting"
	"github.com/PeerDB-io/peerdb/flow/connectors"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils/monitoring"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

type SlotSnapshotState struct {
	connector    connectors.CDCPullConnectorCore
	slotConn     interface{ Close(context.Context) error }
	snapshotName string
}

type TxSnapshotState struct {
	SnapshotName     string
	SupportsTIDScans bool
}

type SnapshotActivity struct {
	Alerter             *alerting.Alerter
	CatalogPool         shared.CatalogPool
	SlotSnapshotStates  map[string]SlotSnapshotState
	TxSnapshotStates    map[string]TxSnapshotState
	SnapshotStatesMutex sync.Mutex
}

// closes the slot signal
func (a *SnapshotActivity) CloseSlotKeepAlive(ctx context.Context, flowJobName string) error {
	a.SnapshotStatesMutex.Lock()
	defer a.SnapshotStatesMutex.Unlock()

	if s, ok := a.SlotSnapshotStates[flowJobName]; ok {
		if s.slotConn != nil {
			s.slotConn.Close(ctx)
		}
		connectors.CloseConnector(ctx, s.connector)
		delete(a.SlotSnapshotStates, flowJobName)
	}
	a.Alerter.LogFlowEvent(ctx, flowJobName, "Ended Snapshot Flow Job")
	return nil
}

func (a *SnapshotActivity) InitializeSnapshot(
	ctx context.Context, flowName string,
) (int32, error) {
	ctx = context.WithValue(ctx, shared.FlowNameKey, flowName)
	logger := internal.LoggerFromCtx(ctx)

	snapshotID, err := monitoring.InitializeSnapshot(ctx, logger, a.CatalogPool, flowName)
	if err != nil {
		return -1, a.Alerter.LogFlowErrorNoStatus(ctx, flowName, err)
	}
	return snapshotID, nil
}

func (a *SnapshotActivity) FinishSnapshot(
	ctx context.Context, flowName string, snapshotID int32,
) error {
	ctx = context.WithValue(ctx, shared.FlowNameKey, flowName)
	logger := internal.LoggerFromCtx(ctx)
	if err := monitoring.FinishSnapshot(ctx, logger, a.CatalogPool, flowName, snapshotID); err != nil {
		return a.Alerter.LogFlowSnapshotError(ctx, flowName, snapshotID, err)
	}
	return nil
}

func (a *SnapshotActivity) SetupReplication(
	ctx context.Context,
	config *protos.SetupReplicationInput,
) (*protos.SetupReplicationOutput, error) {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	logger := internal.LoggerFromCtx(ctx)
	a.Alerter.LogFlowInfo(ctx, config.FlowJobName, "Setting up replication slot and publication")
	a.Alerter.LogFlowEvent(ctx, config.FlowJobName, "Started Snapshot Flow Job")

	conn, err := connectors.GetByNameAs[connectors.CDCPullConnectorCore](ctx, nil, a.CatalogPool, config.PeerName)
	if err != nil {
		conErr := fmt.Errorf("failed to get connector: %w", err)
		return nil, a.Alerter.LogFlowSnapshotError(ctx, config.FlowJobName, config.SnapshotId, conErr)
	}

	logger.Info("waiting for slot to be created...")
	slotInfo, err := conn.SetupReplication(ctx, config)

	if err != nil {
		connectors.CloseConnector(ctx, conn)
		// it is important to close the connection here as it is not closed in CloseSlotKeepAlive
		slotErr := fmt.Errorf("slot error: %w", err)
		return nil, a.Alerter.LogFlowSnapshotError(ctx, config.FlowJobName, config.SnapshotId, slotErr)
	} else if slotInfo.Conn == nil && slotInfo.SlotName == "" {
		connectors.CloseConnector(ctx, conn)
		logger.Info("replication setup without slot")
		return nil, nil
	} else {
		logger.Info("slot created", slog.String("SlotName", slotInfo.SlotName))
	}

	a.SnapshotStatesMutex.Lock()
	defer a.SnapshotStatesMutex.Unlock()

	a.SlotSnapshotStates[config.FlowJobName] = SlotSnapshotState{
		slotConn:     slotInfo.Conn,
		snapshotName: slotInfo.SnapshotName,
		connector:    conn,
	}

	a.Alerter.LogFlowInfo(ctx, config.FlowJobName, "Replication slot and publication setup complete")

	return &protos.SetupReplicationOutput{
		SlotName:         slotInfo.SlotName,
		SnapshotName:     slotInfo.SnapshotName,
		SupportsTidScans: slotInfo.SupportsTIDScans,
	}, nil
}

func (a *SnapshotActivity) MaintainTx(ctx context.Context, sessionID string, peer string, env map[string]string, snapshotID int32) error {
	shutdown := heartbeatRoutine(ctx, func() string {
		return "maintaining transaction snapshot"
	})
	defer shutdown()
	conn, err := connectors.GetByNameAs[connectors.CDCPullConnector](ctx, nil, a.CatalogPool, peer)
	if err != nil {
		getErr := fmt.Errorf("failed to get connector: %w", err)
		return a.Alerter.LogFlowSnapshotError(ctx, sessionID, snapshotID, getErr)
	}
	defer connectors.CloseConnector(ctx, conn)

	exportSnapshotOutput, tx, err := conn.ExportTxSnapshot(ctx, env)
	if err != nil {
		exportErr := fmt.Errorf("failed to export tx snapshot: %w", err)
		return a.Alerter.LogFlowSnapshotError(ctx, sessionID, snapshotID, exportErr)
	}

	a.SnapshotStatesMutex.Lock()
	if exportSnapshotOutput != nil {
		a.TxSnapshotStates[sessionID] = TxSnapshotState{
			SnapshotName:     exportSnapshotOutput.SnapshotName,
			SupportsTIDScans: exportSnapshotOutput.SupportsTidScans,
		}
	} else {
		a.TxSnapshotStates[sessionID] = TxSnapshotState{}
	}
	a.SnapshotStatesMutex.Unlock()

	logger := internal.LoggerFromCtx(ctx)
	start := time.Now()
	for {
		logger.Info("maintaining export snapshot transaction", slog.Int64("seconds", int64(time.Since(start).Round(time.Second)/time.Second)))
		if ctx.Err() != nil {
			a.SnapshotStatesMutex.Lock()
			delete(a.TxSnapshotStates, sessionID)
			a.SnapshotStatesMutex.Unlock()
			if err := conn.FinishExport(tx); err != nil {
				finishErr := fmt.Errorf("failed to finish export: %w", err)
				return a.Alerter.LogFlowSnapshotError(ctx, sessionID, snapshotID, finishErr)
			}
			return nil
		}
		time.Sleep(time.Minute)
	}
}

func (a *SnapshotActivity) WaitForExportSnapshot(ctx context.Context, sessionID string) (*TxSnapshotState, error) {
	logger := internal.LoggerFromCtx(ctx)
	attempt := 0
	for {
		a.SnapshotStatesMutex.Lock()
		tsc, ok := a.TxSnapshotStates[sessionID]
		a.SnapshotStatesMutex.Unlock()
		if ok {
			return &tsc, nil
		}
		activity.RecordHeartbeat(ctx, "wait another second for snapshot export")
		attempt += 1
		if attempt > 2 {
			logger.Info("waiting on snapshot export", slog.Int("attempt", attempt))
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		time.Sleep(time.Second)
	}
}

func (a *SnapshotActivity) LoadTableSchema(
	ctx context.Context,
	flowName string,
	tableName string,
	snapshotID int32,
) (*protos.TableSchema, error) {
	schema, err := internal.LoadTableSchemaFromCatalog(ctx, a.CatalogPool, flowName, tableName)
	if err != nil {
		loadErr := fmt.Errorf("failed to load schema from catalog: %w", err)
		return nil, a.Alerter.LogFlowSnapshotError(ctx, flowName, snapshotID, loadErr)
	}
	return schema, nil
}

func (a *SnapshotActivity) ParseSchemaTable(
	ctx context.Context,
	flowName string,
	tableName string,
	snapshotID int32,
) (*utils.SchemaTable, error) {
	parsedTable, err := utils.ParseSchemaTable(tableName)
	if err != nil {
		parseErr := fmt.Errorf("failed to parse schema table: %w", err)
		return nil, a.Alerter.LogFlowSnapshotError(ctx, flowName, snapshotID, parseErr)
	}
	return parsedTable, nil
}

func (a *SnapshotActivity) GetPeerType(
	ctx context.Context,
	flowName string,
	peerName string,
	snapshotID int32,
) (protos.DBType, error) {
	dbtype, err := connectors.LoadPeerType(ctx, a.CatalogPool, peerName)
	if err != nil {
		peerErr := fmt.Errorf("failed to get peer type: %w", err)
		return 0, a.Alerter.LogFlowSnapshotError(ctx, flowName, snapshotID, peerErr)
	}
	return dbtype, nil
}
