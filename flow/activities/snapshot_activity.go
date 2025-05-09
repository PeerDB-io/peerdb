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
		return nil, a.Alerter.LogFlowError(ctx, config.FlowJobName, fmt.Errorf("failed to get connector: %w", err))
	}

	logger.Info("waiting for slot to be created...")
	slotInfo, err := conn.SetupReplication(ctx, config)

	if err != nil {
		connectors.CloseConnector(ctx, conn)
		// it is important to close the connection here as it is not closed in CloseSlotKeepAlive
		return nil, a.Alerter.LogFlowError(ctx, config.FlowJobName, fmt.Errorf("slot error: %w", err))
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

func (a *SnapshotActivity) MaintainTx(ctx context.Context, sessionID string, peer string, env map[string]string) error {
	shutdown := heartbeatRoutine(ctx, func() string {
		return "maintaining transaction snapshot"
	})
	defer shutdown()
	conn, err := connectors.GetByNameAs[connectors.CDCPullConnector](ctx, nil, a.CatalogPool, peer)
	if err != nil {
		return a.Alerter.LogFlowError(ctx, sessionID, err)
	}
	defer connectors.CloseConnector(ctx, conn)

	exportSnapshotOutput, tx, err := conn.ExportTxSnapshot(ctx, env)
	if err != nil {
		return err
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
			return conn.FinishExport(tx)
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
) (*protos.TableSchema, error) {
	return internal.LoadTableSchemaFromCatalog(ctx, a.CatalogPool, flowName, tableName)
}
