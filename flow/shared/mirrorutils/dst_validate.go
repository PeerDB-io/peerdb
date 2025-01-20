package mirrorutils

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/PeerDB-io/peerdb/flow/alerting"
	"github.com/PeerDB-io/peerdb/flow/connectors"
	connclickhouse "github.com/PeerDB-io/peerdb/flow/connectors/clickhouse"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/telemetry"
)

var (
	CustomColumnTypeRegex = regexp.MustCompile(`^$|^[a-zA-Z][a-zA-Z0-9(),]*$`)
	CustomColumnNameRegex = regexp.MustCompile(`^$|^[a-zA-Z_][a-zA-Z0-9_]*$`)
)

func ClickHouseDestinationMirrorValidate(ctx context.Context, catalogPool *pgxpool.Pool,
	alerter *alerting.Alerter, cfg *protos.FlowConnectionConfigs,
) error {
	srcPeer, err := connectors.GetByNameAs[connectors.GetTableSchemaConnector](ctx, nil, catalogPool, cfg.SourceName)
	if err != nil {
		return err
	}
	defer srcPeer.Close()

	chPeer, err := connectors.GetByNameAs[*connclickhouse.ClickHouseConnector](ctx, nil, catalogPool, cfg.DestinationName)
	if err != nil {
		if errors.Is(err, errors.ErrUnsupported) {
			slog.Error("source is not a ClickHouse peer", slog.String("peer", cfg.SourceName))
			return errors.New("source is not a ClickHouse peer")
		}
		displayErr := fmt.Errorf("failed to create clickhouse connector: %v", err)
		alerter.LogNonFlowWarning(ctx, telemetry.CreateMirror, cfg.FlowJobName, displayErr.Error())
		return fmt.Errorf("failed to create clickhouse connector: %w", err)
	}
	defer chPeer.Close()

	srcTableNames := make([]string, 0, len(cfg.TableMappings))
	for _, tableMapping := range cfg.TableMappings {
		srcTableNames = append(srcTableNames, tableMapping.SourceTableIdentifier)
	}

	res, err := srcPeer.GetTableSchema(ctx, nil, cfg.System, srcTableNames)
	if err != nil {
		displayErr := fmt.Errorf("failed to get source table schema: %v", err)
		alerter.LogNonFlowWarning(ctx, telemetry.CreateMirror, cfg.FlowJobName, displayErr.Error())
		return displayErr
	}

	err = chPeer.CheckDestinationTables(ctx, cfg, res)
	if err != nil {
		alerter.LogNonFlowWarning(ctx, telemetry.CreateMirror, cfg.FlowJobName, err.Error())
		return err
	}

	for _, tm := range cfg.TableMappings {
		for _, col := range tm.Columns {
			if !CustomColumnTypeRegex.MatchString(col.DestinationType) {
				return fmt.Errorf("invalid custom column type %s", col.DestinationType)
			}
			if !CustomColumnNameRegex.MatchString(col.DestinationName) {
				return fmt.Errorf("invalid custom column name %s", col.DestinationName)
			}
		}
	}

	return nil
}
