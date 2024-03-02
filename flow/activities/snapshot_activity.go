package activities

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.temporal.io/sdk/activity"

	"github.com/PeerDB-io/peer-flow/alerting"
	"github.com/PeerDB-io/peer-flow/connectors"
	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
)

type SnapshotActivity struct {
	SnapshotConnectionsMutex sync.Mutex
	SnapshotConnections      map[string]SlotSnapshotSignal
	Alerter                  *alerting.Alerter
}

// closes the slot signal
func (a *SnapshotActivity) CloseSlotKeepAlive(ctx context.Context, flowJobName string) error {
	a.SnapshotConnectionsMutex.Lock()
	defer a.SnapshotConnectionsMutex.Unlock()

	if s, ok := a.SnapshotConnections[flowJobName]; ok {
		close(s.signal.CloneComplete)
		connectors.CloseConnector(ctx, s.connector)
		delete(a.SnapshotConnections, flowJobName)
	}

	return nil
}

func (a *SnapshotActivity) SetupReplication(
	ctx context.Context,
	config *protos.SetupReplicationInput,
) (*protos.SetupReplicationOutput, error) {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	logger := activity.GetLogger(ctx)

	dbType := config.PeerConnectionConfig.Type
	if dbType != protos.DBType_POSTGRES {
		logger.Info(fmt.Sprintf("setup replication is no-op for %s", dbType))
		return nil, nil
	}

	conn, err := connectors.GetCDCPullConnector(ctx, config.PeerConnectionConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}

	slotSignal := connpostgres.NewSlotSignal()

	replicationErr := make(chan error)
	defer close(replicationErr)

	closeConnectionForError := func(err error) {
		logger.Error("failed to setup replication", slog.Any("error", err))
		a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
		// it is important to close the connection here as it is not closed in CloseSlotKeepAlive
		connectors.CloseConnector(ctx, conn)
	}

	// This now happens in a goroutine
	go func() {
		pgConn := conn.(*connpostgres.PostgresConnector)
		err = pgConn.SetupReplication(ctx, slotSignal, config)
		if err != nil {
			closeConnectionForError(err)
			replicationErr <- err
			return
		}
	}()

	logger.Info("waiting for slot to be created...")
	var slotInfo connpostgres.SlotCreationResult
	select {
	case slotInfo = <-slotSignal.SlotCreated:
		logger.Info("slot created", slotInfo.SlotName)
	case err := <-replicationErr:
		closeConnectionForError(err)
		return nil, fmt.Errorf("failed to setup replication: %w", err)
	}

	if slotInfo.Err != nil {
		closeConnectionForError(slotInfo.Err)
		return nil, fmt.Errorf("slot error: %w", slotInfo.Err)
	}

	a.SnapshotConnectionsMutex.Lock()
	defer a.SnapshotConnectionsMutex.Unlock()

	a.SnapshotConnections[config.FlowJobName] = SlotSnapshotSignal{
		signal:       slotSignal,
		snapshotName: slotInfo.SnapshotName,
		connector:    conn,
	}

	return &protos.SetupReplicationOutput{
		SlotName:     slotInfo.SlotName,
		SnapshotName: slotInfo.SnapshotName,
	}, nil
}

func (a *SnapshotActivity) MaintainTx(ctx context.Context, sessionID string, peer *protos.Peer) error {
	conn, err := connectors.GetCDCPullConnector(ctx, peer)
	if err != nil {
		return err
	}
	defer connectors.CloseConnector(ctx, conn)

	snapshotName, tx, err := conn.ExportSnapshot(ctx)
	if err != nil {
		return err
	}

	sss := SlotSnapshotSignal{snapshotName: snapshotName}
	a.SnapshotConnectionsMutex.Lock()
	a.SnapshotConnections[sessionID] = sss
	a.SnapshotConnectionsMutex.Unlock()

	logger := activity.GetLogger(ctx)
	start := time.Now()
	for {
		msg := fmt.Sprintf("maintaining export snapshot transaction %s", time.Since(start).Round(time.Second))
		logger.Info(msg)
		activity.RecordHeartbeat(ctx, msg)
		if ctx.Err() != nil {
			a.SnapshotConnectionsMutex.Lock()
			delete(a.SnapshotConnections, sessionID)
			a.SnapshotConnectionsMutex.Unlock()
			return conn.FinishExport(tx)
		}
		time.Sleep(time.Minute)
	}
}

func (a *SnapshotActivity) WaitForExportSnapshot(ctx context.Context, sessionID string) (string, error) {
	logger := activity.GetLogger(ctx)
	attempt := 0
	for {
		a.SnapshotConnectionsMutex.Lock()
		sss, ok := a.SnapshotConnections[sessionID]
		a.SnapshotConnectionsMutex.Unlock()
		if ok {
			return sss.snapshotName, nil
		}
		activity.RecordHeartbeat(ctx, "wait another second for snapshot export")
		attempt += 1
		if attempt > 2 {
			logger.Info("waiting on snapshot export", slog.Int("attempt", attempt))
		}
		if err := ctx.Err(); err != nil {
			return "", err
		}
		time.Sleep(time.Second)
	}
}
