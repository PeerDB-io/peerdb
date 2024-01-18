package activities

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/PeerDB-io/peer-flow/connectors"
	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
	"github.com/PeerDB-io/peer-flow/shared/alerting"
)

type SnapshotActivity struct {
	SnapshotConnections map[string]SlotSnapshotSignal
	Alerter             *alerting.Alerter
}

// closes the slot signal
func (a *SnapshotActivity) CloseSlotKeepAlive(flowJobName string) error {
	if a.SnapshotConnections == nil {
		return nil
	}

	if s, ok := a.SnapshotConnections[flowJobName]; ok {
		close(s.signal.CloneComplete)
		s.connector.Close()
	}

	return nil
}

func (a *SnapshotActivity) SetupReplication(
	ctx context.Context,
	config *protos.SetupReplicationInput,
) (*protos.SetupReplicationOutput, error) {
	ctx = context.WithValue(ctx, shared.FlowNameKey, config.FlowJobName)
	dbType := config.PeerConnectionConfig.Type
	if dbType != protos.DBType_POSTGRES {
		slog.InfoContext(ctx, fmt.Sprintf("setup replication is no-op for %s", dbType))
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
		slog.ErrorContext(ctx, "failed to setup replication", slog.Any("error", err))
		a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
		// it is important to close the connection here as it is not closed in CloseSlotKeepAlive
		connCloseErr := conn.Close()
		if connCloseErr != nil {
			slog.ErrorContext(ctx, "failed to close connection", slog.Any("error", connCloseErr))
		}
	}

	// This now happens in a goroutine
	go func() {
		pgConn := conn.(*connpostgres.PostgresConnector)
		err = pgConn.SetupReplication(slotSignal, config)
		if err != nil {
			closeConnectionForError(err)
			replicationErr <- err
			return
		}
	}()

	slog.InfoContext(ctx, "waiting for slot to be created...")
	var slotInfo connpostgres.SlotCreationResult
	select {
	case slotInfo = <-slotSignal.SlotCreated:
		slog.InfoContext(ctx, fmt.Sprintf("slot '%s' created", slotInfo.SlotName))
	case err := <-replicationErr:
		closeConnectionForError(err)
		return nil, fmt.Errorf("failed to setup replication: %w", err)
	}

	if slotInfo.Err != nil {
		closeConnectionForError(slotInfo.Err)
		return nil, fmt.Errorf("slot error: %w", slotInfo.Err)
	}

	if a.SnapshotConnections == nil {
		a.SnapshotConnections = make(map[string]SlotSnapshotSignal)
	}

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
