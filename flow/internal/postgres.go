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
	"github.com/jackc/pgx/v5"
)

func GetPGConnectionString(pgConfig *protos.PostgresConfig, flowName string) string {
	passwordEscaped := url.QueryEscape(pgConfig.Password)
	applicationName := "peerdb"
	if flowName != "" {
		applicationName = "peerdb_" + flowName
	}

	// for a url like postgres://user:password@host:port/dbname
	connString := fmt.Sprintf(
		"postgres://%s:%s@%s/%s?application_name=%s&client_encoding=UTF8",
		pgConfig.User,
		passwordEscaped,
		shared.JoinHostPort(pgConfig.Host, pgConfig.Port),
		pgConfig.Database,
		applicationName,
	)
	if pgConfig.RequireTls {
		connString += "&sslmode=require"
	}
	return connString
}

func UpdateCDCConfigInCatalog(ctx context.Context, pool shared.CatalogPool,
	logger log.Logger, cfg *protos.FlowConnectionConfigsCore,
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

func UpdateTableOIDsInTableSchemaInCatalog(
	ctx context.Context,
	pool shared.CatalogPool,
	logger log.Logger,
	flowName string,
	tableOIDs map[string]uint32, // map[destinationTableName]tableOID
) error {
	logger.Info("updating table OIDs in catalog",
		slog.String("flowName", flowName),
		slog.Int("numTables", len(tableOIDs)))

	if len(tableOIDs) == 0 {
		return nil
	}

	tableNames := make([]string, 0, len(tableOIDs))
	for tableName := range tableOIDs {
		tableNames = append(tableNames, tableName)
	}
	tableSchemas, err := loadTableSchemasFromCatalog(ctx, pool, flowName, tableNames)
	if err != nil {
		return fmt.Errorf("failed to load table schemas from catalog: %w", err)
	}

	tx, err := pool.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer shared.RollbackTx(tx, logger)

	batch := &pgx.Batch{}
	for tableName, tableOID := range tableOIDs {
		tableSchema, exists := tableSchemas[tableName]
		if !exists {
			logger.Error("table schema not found in catalog",
				slog.String("flowName", flowName),
				slog.String("tableName", tableName))
			return fmt.Errorf("table schema not found for table: %s", tableName)
		}

		tableSchema.TableOid = tableOID
		tableSchemaBytes, err := proto.Marshal(tableSchema)
		if err != nil {
			return fmt.Errorf("unable to marshal updated table schema for %s: %w", tableName, err)
		}

		batch.Queue(
			"UPDATE table_schema_mapping SET table_schema=$1 WHERE flow_name=$2 AND table_name=$3",
			tableSchemaBytes, flowName, tableName,
		)

		logger.Info("queued table OID update",
			slog.String("flowName", flowName),
			slog.String("tableName", tableName),
			slog.Uint64("tableOID", uint64(tableOID)))
	}

	results := tx.SendBatch(ctx, batch)
	defer results.Close()

	for i := 0; i < len(tableOIDs); i++ {
		_, err := results.Exec()
		if err != nil {
			logger.Error("failed to update table schema in catalog",
				slog.Any("error", err),
				slog.String("flowName", flowName),
				slog.Int("batchIndex", i))
			return fmt.Errorf("failed to update table schema in catalog: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	logger.Info("successfully updated all table OIDs in catalog",
		slog.String("flowName", flowName),
		slog.Int("numTables", len(tableOIDs)))

	return nil
}

func loadTableSchemasFromCatalog(
	ctx context.Context,
	pool shared.CatalogPool,
	flowName string,
	tableNames []string,
) (map[string]*protos.TableSchema, error) {
	if len(tableNames) == 0 {
		return make(map[string]*protos.TableSchema), nil
	}

	rows, err := pool.Pool.Query(
		ctx,
		"SELECT table_name, table_schema FROM table_schema_mapping WHERE flow_name = $1 AND table_name = ANY($2)",
		flowName,
		tableNames,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query table schemas: %w", err)
	}
	defer rows.Close()

	schemas := make(map[string]*protos.TableSchema)

	for rows.Next() {
		var tableName string
		var tableSchemaBytes []byte

		if err := rows.Scan(&tableName, &tableSchemaBytes); err != nil {
			return nil, fmt.Errorf("failed to scan table schema row: %w", err)
		}

		tableSchema := &protos.TableSchema{}
		if err := proto.Unmarshal(tableSchemaBytes, tableSchema); err != nil {
			return nil, fmt.Errorf("failed to unmarshal table schema for %s: %w", tableName, err)
		}

		schemas[tableName] = tableSchema
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating table schema rows: %w", err)
	}

	return schemas, nil
}
