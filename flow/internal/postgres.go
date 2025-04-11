package internal

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"

	"go.temporal.io/sdk/log"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func GetPGConnectionString(pgConfig *protos.PostgresConfig, flowName string) string {
	passwordEscaped := url.QueryEscape(pgConfig.Password)
	applicationName := "peerdb"
	if flowName != "" {
		applicationName = "peerdb_" + flowName
	}

	// for a url like postgres://user:password@host:port/dbname
	connString := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?application_name=%s&client_encoding=UTF8",
		pgConfig.User,
		passwordEscaped,
		pgConfig.Host,
		pgConfig.Port,
		pgConfig.Database,
		applicationName,
	)
	if pgConfig.RequireTls {
		connString += "&sslmode=require"
	}
	return connString
}

func UpdateCDCConfigInCatalog(ctx context.Context, pool shared.CatalogPool,
	logger log.Logger, cfg *protos.FlowConnectionConfigs,
) error {
	logger.Info("syncing state to catalog: updating config_proto in flows", slog.String("flowName", cfg.FlowJobName))

	cfgBytes, err := proto.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("unable to marshal flow config: %w", err)
	}

	if _, err := pool.Exec(ctx, "UPDATE flows SET config_proto=$1,updated_at=now() WHERE name=$2", cfgBytes, cfg.FlowJobName); err != nil {
		logger.Error("failed to update catalog", slog.Any("error", err), slog.String("flowName", cfg.FlowJobName))
		return fmt.Errorf("failed to update catalog: %w", err)
	}

	logger.Info("synced state to catalog: updated config_proto in flows", slog.String("flowName", cfg.FlowJobName))
	return nil
}

func LoadTableSchemaFromCatalog(
	ctx context.Context,
	pool shared.CatalogPool,
	flowName string,
	tableName string,
) (*protos.TableSchema, error) {
	var tableSchemaBytes []byte
	if err := pool.Pool.QueryRow(
		ctx,
		"select table_schema from table_schema_mapping where flow_name = $1 and table_name = $2",
		flowName,
		tableName,
	).Scan(&tableSchemaBytes); err != nil {
		return nil, err
	}
	tableSchema := &protos.TableSchema{}
	return tableSchema, proto.Unmarshal(tableSchemaBytes, tableSchema)
}
