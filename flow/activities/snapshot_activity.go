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
	"golang.org/x/sync/errgroup"
)

type SlotSnapshotSignal struct {
	signal       connpostgres.SnapshotSignal
	snapshotName string
	connector    connectors.CDCPullConnector
}

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
		s.signal.CloneComplete <- struct{}{}
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

	errGroup, errCtx := errgroup.WithContext(ctx)

	conn, err := connectors.GetCDCPullConnector(errCtx, config.PeerConnectionConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to get connector: %w", err)
	}

	snapshotSignal := connpostgres.NewSnapshotSignal()
	errGroup.Go(func() error {
		pgConn := conn.(*connpostgres.PostgresConnector)
		return pgConn.SetupReplication(snapshotSignal, config)
	})

	slog.InfoContext(ctx, "waiting for slot to be created...")
	var slotInfo connpostgres.SnapshotCreationResult
	select {
	case slotInfo = <-snapshotSignal.SlotCreated:
		slog.InfoContext(ctx, fmt.Sprintf("slot '%s' created", slotInfo.SlotName))
	case err := <-snapshotSignal.Error:
		a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
		return nil, fmt.Errorf("failed to setup replication: %w", err)
	}

	if errWait := errGroup.Wait(); errWait != nil {
		a.Alerter.LogFlowError(ctx, config.FlowJobName, errWait)
		return nil, fmt.Errorf("failed to setup replication: %w", errWait)
	}

	if a.SnapshotConnections == nil {
		a.SnapshotConnections = make(map[string]SlotSnapshotSignal)
	}

	a.SnapshotConnections[config.FlowJobName] = SlotSnapshotSignal{
		signal:       snapshotSignal,
		snapshotName: slotInfo.SnapshotName,
		connector:    conn,
	}

	return &protos.SetupReplicationOutput{
		SlotName:     slotInfo.SlotName,
		SnapshotName: slotInfo.SnapshotName,
	}, nil
}
