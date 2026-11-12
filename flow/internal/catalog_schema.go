package internal

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"
	"go.temporal.io/sdk/log"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

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

func ReadModifyWriteTableSchemasToCatalog(
	ctx context.Context,
	catalogPool shared.CatalogPool,
	logger log.Logger,
	flowName string,
	tableNames []string,
	modifyFn func(map[string]*protos.TableSchema) (map[string]*protos.TableSchema, error),
) error {
	tx, err := catalogPool.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer shared.RollbackTx(tx, logger)

	if err := ReadModifyWriteTableSchemasToCatalogTx(ctx, tx, logger, flowName, tableNames, modifyFn); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

// ReadModifyWriteTableSchemasToCatalogTx leaves commit and rollback to the caller.
func ReadModifyWriteTableSchemasToCatalogTx(ctx context.Context, tx pgx.Tx, logger log.Logger, flowName string,
	tableNames []string, modifyFn func(map[string]*protos.TableSchema) (map[string]*protos.TableSchema, error),
) error {
	// read
	tableSchemas, err := LoadTableSchemasFromCatalog(ctx, tx, flowName, tableNames)
	if err != nil {
		return fmt.Errorf("failed to load table schemas from catalog: %w", err)
	}
	if len(tableSchemas) != len(tableNames) {
		filteredTableNames := make([]string, 0, len(tableSchemas))
		for name := range tableSchemas {
			filteredTableNames = append(filteredTableNames, name)
		}
		logger.Warn("not all tables are found in catalog",
			slog.String("expected", strings.Join(tableNames, ", ")),
			slog.String("actual", strings.Join(filteredTableNames, ", ")))
	}

	// modify
	modifiedTableSchemas, err := modifyFn(tableSchemas)
	if err != nil {
		return fmt.Errorf("failed to modify table schemas from catalog: %w", err)
	}

	// write (in batch)
	batch := &pgx.Batch{}
	for tableName, tableSchema := range modifiedTableSchemas {
		tableSchemaBytes, err := proto.Marshal(tableSchema)
		if err != nil {
			return fmt.Errorf("unable to marshal table schema for %s: %w", tableName, err)
		}
		batch.Queue(
			"UPDATE table_schema_mapping SET table_schema=$1 WHERE flow_name=$2 AND table_name=$3",
			tableSchemaBytes, flowName, tableName,
		)
		logger.Info("queued schema delta update",
			slog.String("flowName", flowName),
			slog.String("tableName", tableName))
	}
	results := tx.SendBatch(ctx, batch)
	defer results.Close() // Ensure resources are freed in case of early return

	for tableName := range modifiedTableSchemas {
		if _, err := results.Exec(); err != nil {
			logger.Error("failed to update table schema in catalog",
				slog.Any("error", err),
				slog.String("flowName", flowName),
				slog.String("tableName", tableName))
			return fmt.Errorf("failed to update table schema in catalog: %w", err)
		}
	}

	// Close results before committing
	if err := results.Close(); err != nil {
		logger.Error("failed to close batch results",
			slog.Any("error", err),
			slog.String("flowName", flowName))
		return fmt.Errorf("failed to close batch results: %w", err)
	}

	return nil
}

// support both catalog pool and pgx.Tx
type CatalogQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func LoadTableSchemasFromCatalog(
	ctx context.Context,
	querier CatalogQuerier,
	flowName string,
	tableNames []string,
) (map[string]*protos.TableSchema, error) {
	if len(tableNames) == 0 {
		return make(map[string]*protos.TableSchema), nil
	}

	rows, err := querier.Query(
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
