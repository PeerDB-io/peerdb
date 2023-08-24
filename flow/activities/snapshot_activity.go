package activities

import (
	"context"
	"fmt"

	"github.com/PeerDB-io/peer-flow/connectors"
	connpostgres "github.com/PeerDB-io/peer-flow/connectors/postgres"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	log "github.com/sirupsen/logrus"
)

type SnapshotActivity struct {
	EnableMetrics       bool
	SnapshotConnections map[string]*SlotSnapshotSignal
}

// closes the slot signal
func (a *SnapshotActivity) CloseSlotKeepAlive(flowJobName string) error {
	if a.SnapshotConnections == nil {
		return nil
	}

	if s, ok := a.SnapshotConnections[flowJobName]; ok {
		s.signal.CloneComplete <- true
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
		log.Infof("setup replication is no-op for %s", dbType)
		return nil, nil
	}

	conn, err := connectors.GetConnector(ctx, config.PeerConnectionConfig)
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
			log.WithFields(log.Fields{
				"flowName":            config.FlowJobName,
				"destinationPeerName": config.DestinationPeer.Name,
			}).Errorf("failed to setup replication: %v", err)
			replicationErr <- err
			return
		}
	}()

	log.WithFields(log.Fields{
		"flowName":            config.FlowJobName,
		"destinationPeerName": config.DestinationPeer.Name,
	}).Info("waiting for slot to be created...")
	var slotInfo *connpostgres.SlotCreationResult
	select {
	case slotInfo = <-slotSignal.SlotCreated:
		log.WithFields(log.Fields{
			"flowName":            config.FlowJobName,
			"destinationPeerName": config.DestinationPeer.Name,
		}).Infof("slot '%s' created", slotInfo.SlotName)
	case err := <-replicationErr:
		return nil, fmt.Errorf("failed to setup replication: %w", err)
	}

	if slotInfo.Err != nil {
		return nil, fmt.Errorf("slot error: %w", slotInfo.Err)
	}

	if a.SnapshotConnections == nil {
		a.SnapshotConnections = make(map[string]*SlotSnapshotSignal)
	}

	a.SnapshotConnections[config.FlowJobName] = &SlotSnapshotSignal{
		signal:       slotSignal,
		snapshotName: slotInfo.SnapshotName,
		connector:    conn,
	}

	return &protos.SetupReplicationOutput{
		SlotName:     slotInfo.SlotName,
		SnapshotName: slotInfo.SnapshotName,
	}, nil
}
