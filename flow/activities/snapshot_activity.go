package activities

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/activity"

	"github.com/PeerDB-io/peer-flow/alerting"
	"github.com/PeerDB-io/peer-flow/connectors"
	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
)

type SlotSnapshotState struct {
	connector    connectors.CDCPullConnector
	signal       connpostgres.SlotSignal
	snapshotName string
}

type TxSnapshotState struct {
	SnapshotName     string
	SupportsTIDScans bool
}

type SnapshotActivity struct {
	Alerter             *alerting.Alerter
	CatalogPool         *pgxpool.Pool
	SlotSnapshotStates  map[string]SlotSnapshotState
	TxSnapshotStates    map[string]TxSnapshotState
	SnapshotStatesMutex sync.Mutex
}

// closes the slot signal
func (a *SnapshotActivity) CloseSlotKeepAlive(ctx context.Context, flowJobName string) error {
	a.SnapshotStatesMutex.Lock()
	defer a.SnapshotStatesMutex.Unlock()

	if s, ok := a.SlotSnapshotStates[flowJobName]; ok {
		close(s.signal.CloneComplete)
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
	logger := activity.GetLogger(ctx)

	a.Alerter.LogFlowEvent(ctx, config.FlowJobName, "Started Snapshot Flow Job")

	conn, err := connectors.GetByNameAs[*connpostgres.PostgresConnector](ctx, nil, a.CatalogPool, config.PeerName)
	if err != nil {
		if errors.Is(err, errors.ErrUnsupported) {
			logger.Info("setup replication is no-op for non-postgres source")
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}

	closeConnectionForError := func(err error) {
		a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
		// it is important to close the connection here as it is not closed in CloseSlotKeepAlive
		connectors.CloseConnector(ctx, conn)
	}

	slotSignal := connpostgres.NewSlotSignal()
	go conn.SetupReplication(ctx, slotSignal, config)

	logger.Info("waiting for slot to be created...")
	slotInfo := <-slotSignal.SlotCreated

	if slotInfo.Err != nil {
		closeConnectionForError(slotInfo.Err)
		return nil, fmt.Errorf("slot error: %w", slotInfo.Err)
	} else {
		logger.Info("slot created", slog.String("SlotName", slotInfo.SlotName))
	}

	a.SnapshotStatesMutex.Lock()
	defer a.SnapshotStatesMutex.Unlock()

	a.SlotSnapshotStates[config.FlowJobName] = SlotSnapshotState{
		signal:       slotSignal,
		snapshotName: slotInfo.SnapshotName,
		connector:    conn,
	}

	return &protos.SetupReplicationOutput{
		SlotName:         slotInfo.SlotName,
		SnapshotName:     slotInfo.SnapshotName,
		SupportsTidScans: slotInfo.SupportsTIDScans,
	}, nil
}

func (a *SnapshotActivity) MaintainTx(ctx context.Context, sessionID string, peer string) error {
	conn, err := connectors.GetByNameAs[connectors.CDCPullConnector](ctx, nil, a.CatalogPool, peer)
	if err != nil {
		return err
	}
	defer connectors.CloseConnector(ctx, conn)

	exportSnapshotOutput, tx, err := conn.ExportTxSnapshot(ctx)
	if err != nil {
		return err
	}

	a.SnapshotStatesMutex.Lock()
	a.TxSnapshotStates[sessionID] = TxSnapshotState{
		SnapshotName:     exportSnapshotOutput.SnapshotName,
		SupportsTIDScans: exportSnapshotOutput.SupportsTidScans,
	}
	a.SnapshotStatesMutex.Unlock()

	logger := activity.GetLogger(ctx)
	start := time.Now()
	for {
		msg := fmt.Sprintf("maintaining export snapshot transaction %s", time.Since(start).Round(time.Second))
		logger.Info(msg)
		// this function relies on context cancellation to exit
		// context is not explicitly cancelled, but workflow exit triggers an implicit cancel
		// from activity.RecordBeat
		activity.RecordHeartbeat(ctx, msg)
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
	logger := activity.GetLogger(ctx)
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
	return shared.LoadTableSchemaFromCatalog(ctx, a.CatalogPool, flowName, tableName)
}
