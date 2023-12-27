package activities

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/PeerDB-io/peer-flow/connectors"
	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared/alerting"
)

type SnapshotActivity struct {
	SnapshotConnections map[string]*SlotSnapshotSignal
	Alerter             *alerting.Alerter
}

// closes the slot signal
func (a *SnapshotActivity) CloseSlotKeepAlive(flowJobName string) error {
	if a.SnapshotConnections == nil {
		return nil
	}

	if s, ok := a.SnapshotConnections[flowJobName]; ok {
		s.signal.CloneComplete <- struct{}{}
		s.connector.Close()
	}

	return nil
}

func (a *SnapshotActivity) SetupReplication(
	ctx context.Context,
	config *protos.SetupReplicationInput,
) (*protos.SetupReplicationOutput, error) {
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

	// This now happens in a goroutine
	go func() {
		pgConn := conn.(*connpostgres.PostgresConnector)
		err = pgConn.SetupReplication(slotSignal, config)
		if err != nil {
			slog.ErrorContext(ctx, "failed to setup replication", slog.Any("error", err))
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
		a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
		return nil, fmt.Errorf("failed to setup replication: %w", err)
	}

	if slotInfo.Err != nil {
		a.Alerter.LogFlowError(ctx, config.FlowJobName, slotInfo.Err)
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
