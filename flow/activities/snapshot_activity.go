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
	Alerter *alerting.Alerter
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

	// This now happens in a goroutine
	pgConn := conn.(*connpostgres.PostgresConnector)
	res, err := pgConn.SetupReplication(config)
	if err != nil {
		slog.ErrorContext(ctx, "failed to setup replication", slog.Any("error", err))
		a.Alerter.LogFlowError(ctx, config.FlowJobName, err)
		return nil, fmt.Errorf("failed to setup replication: %w", err)
	}

	return res, nil
}
