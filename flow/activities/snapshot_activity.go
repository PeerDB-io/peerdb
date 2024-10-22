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
	connector connectors.CDCPullConnector
	signal    connpostgres.SlotSignal
}

type SnapshotActivity struct {
	Alerter             *alerting.Alerter
	CatalogPool         *pgxpool.Pool
	SlotSnapshotStates  map[string]SlotSnapshotState
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
) error {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	logger := activity.GetLogger(ctx)

	a.Alerter.LogFlowEvent(ctx, config.FlowJobName, "Started Snapshot Flow Job")

	conn, err := connectors.GetByNameAs[*connpostgres.PostgresConnector](ctx, nil, a.CatalogPool, config.PeerName)
	if err != nil {
		if errors.Is(err, errors.ErrUnsupported) {
			logger.Info("setup replication is no-op for non-postgres source")
			return nil
		}
		return fmt.Errorf("failed to get connector: %w", err)
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
		return fmt.Errorf("slot error: %w", slotInfo.Err)
	} else {
		logger.Info("slot created", slog.String("SlotName", slotInfo.SlotName))
	}

	// TODO if record already exists, need to remove slot
	if _, err := a.CatalogPool.Exec(ctx,
		`insert into snapshot_names (flow_name, slot_name, snapshot_name, supports_tid_scan) values ($1, $2, $3, $4)
		on conflict (flow_name) do update set slot_name = $2, snapshot_name = $3, supports_tid_scan = $4`,
		config.FlowJobName, slotInfo.SlotName, slotInfo.SnapshotName, slotInfo.SupportsTIDScans,
	); err != nil {
		return err
	}

	a.SnapshotStatesMutex.Lock()
	a.SlotSnapshotStates[config.FlowJobName] = SlotSnapshotState{
		signal:    slotSignal,
		connector: conn,
	}
	a.SnapshotStatesMutex.Unlock()

	return nil
}

func (a *SnapshotActivity) MaintainTx(ctx context.Context, sessionID string, flowJobName string, peer string) error {
	conn, err := connectors.GetByNameAs[connectors.CDCPullConnector](ctx, nil, a.CatalogPool, peer)
	if err != nil {
		return err
	}
	defer connectors.CloseConnector(ctx, conn)

	tx, err := conn.ExportTxSnapshot(ctx, flowJobName)
	if err != nil {
		return err
	}

	logger := activity.GetLogger(ctx)
	start := time.Now()
	for {
		msg := fmt.Sprintf("maintaining export snapshot transaction %s", time.Since(start).Round(time.Second))
		logger.Info(msg)
		// this function relies on context cancellation to exit
		// context is not explicitly cancelled, but workflow exit triggers an implicit cancel
		// from activity.RecordHeartBeat
		activity.RecordHeartbeat(ctx, msg)
		if ctx.Err() != nil {
			return conn.FinishExport(tx)
		}
		time.Sleep(time.Minute)
	}
}

func (a *SnapshotActivity) LoadTableSchema(
	ctx context.Context,
	flowName string,
	tableName string,
) (*protos.TableSchema, error) {
	return shared.LoadTableSchemaFromCatalog(ctx, a.CatalogPool, flowName, tableName)
}
