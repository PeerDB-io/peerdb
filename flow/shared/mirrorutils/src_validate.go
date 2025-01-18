package mirrorutils

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/PeerDB-io/peerdb/flow/alerting"
	"github.com/PeerDB-io/peerdb/flow/connectors"
	connpostgres "github.com/PeerDB-io/peerdb/flow/connectors/postgres"
	"github.com/PeerDB-io/peerdb/flow/connectors/utils"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/telemetry"
	"github.com/jackc/pgx/v5/pgxpool"
)

func PostgresSourceMirrorValidate(ctx context.Context, catalogPool *pgxpool.Pool,
	alerter *alerting.Alerter, cfg *protos.FlowConnectionConfigs,
) error {
	pgPeer, err := connectors.GetByNameAs[*connpostgres.PostgresConnector](ctx, nil, catalogPool, cfg.SourceName)
	if err != nil {
		if errors.Is(err, errors.ErrUnsupported) {
			slog.Error("source is not a Postgres peer", slog.String("peer", cfg.SourceName))
			return errors.New("source is not a Postgres peer")
		}
		displayErr := fmt.Errorf("failed to create postgres connector: %v", err)
		alerter.LogNonFlowWarning(ctx, telemetry.CreateMirror, cfg.FlowJobName, displayErr.Error())
		return fmt.Errorf("failed to create postgres connector: %w", err)
	}
	defer pgPeer.Close()

	noCDC := cfg.DoInitialSnapshot && cfg.InitialSnapshotOnly
	if !noCDC {
		// Check replication connectivity
		if err := pgPeer.CheckReplicationConnectivity(ctx); err != nil {
			displayErr := fmt.Errorf("unable to establish replication connectivity: %v", err)
			alerter.LogNonFlowWarning(ctx, telemetry.CreateMirror, cfg.FlowJobName,
				displayErr.Error(),
			)
			return displayErr
		}

		// Check permissions of postgres peer
		if err := pgPeer.CheckReplicationPermissions(ctx); err != nil {
			displayErr := fmt.Errorf("failed to check replication permissions: %v", err)
			alerter.LogNonFlowWarning(ctx, telemetry.CreateMirror, cfg.FlowJobName, displayErr.Error())
			return displayErr
		}
	}

	sourceTables := make([]*utils.SchemaTable, 0, len(cfg.TableMappings))
	for _, tableMapping := range cfg.TableMappings {
		parsedTable, parseErr := utils.ParseSchemaTable(tableMapping.SourceTableIdentifier)
		if parseErr != nil {
			displayErr := fmt.Errorf("invalid source table identifier: %s", parseErr)
			alerter.LogNonFlowWarning(ctx, telemetry.CreateMirror, cfg.FlowJobName, displayErr.Error())
			return displayErr
		}

		sourceTables = append(sourceTables, parsedTable)
	}

	pubName := cfg.PublicationName

	if pubName == "" && !noCDC {
		srcTableNames := make([]string, 0, len(sourceTables))
		for _, srcTable := range sourceTables {
			srcTableNames = append(srcTableNames, srcTable.String())
		}

		if err := pgPeer.CheckPublicationCreationPermissions(ctx, srcTableNames); err != nil {
			displayErr := fmt.Errorf("invalid publication creation permissions: %v", err)
			alerter.LogNonFlowWarning(ctx, telemetry.CreateMirror, cfg.FlowJobName, displayErr.Error())
			return displayErr
		}
	}

	if err := pgPeer.CheckSourceTables(ctx, sourceTables, pubName, noCDC); err != nil {
		displayErr := fmt.Errorf("provided source tables invalidated: %v", err)
		slog.Error(displayErr.Error())
		alerter.LogNonFlowWarning(ctx, telemetry.CreateMirror, cfg.FlowJobName, displayErr.Error())
		return displayErr
	}

	return nil
}
