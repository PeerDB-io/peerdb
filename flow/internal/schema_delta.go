package internal

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

func ApplySchemaDeltasToCatalog(ctx context.Context, catalogPool shared.CatalogPool, flowName string,
	schemaDeltas []*protos.TableSchemaDelta,
) error {
	if len(schemaDeltas) == 0 {
		return nil
	}

	tx, err := catalogPool.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin schema transaction: %w", err)
	}
	defer shared.RollbackTx(tx, LoggerFromCtx(ctx))
	if err := ApplySchemaDeltasToCatalogTx(ctx, tx, flowName, schemaDeltas); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func ApplySchemaDeltasToCatalogTx(ctx context.Context, tx pgx.Tx, flowName string, schemaDeltas []*protos.TableSchemaDelta) error {
	if len(schemaDeltas) == 0 {
		return nil
	}
	logger := LoggerFromCtx(ctx)

	dstTableNamesInDeltas := make([]string, 0, len(schemaDeltas))
	for _, schemaDelta := range schemaDeltas {
		dstTableNamesInDeltas = append(dstTableNamesInDeltas, schemaDelta.DstTableName)
	}

	if err := ReadModifyWriteTableSchemasToCatalogTx(
		ctx,
		tx,
		logger,
		flowName,
		dstTableNamesInDeltas,
		func(schemas map[string]*protos.TableSchema) (map[string]*protos.TableSchema, error) {
			// deep copy to avoid mutating input
			schemasCopy := make(map[string]*protos.TableSchema, len(schemas))
			for tableName, schema := range schemas {
				if schema == nil {
					return nil, fmt.Errorf("failed to deep copy table schema from catalog: table %s has nil schema", tableName)
				}
				schemasCopy[tableName] = proto.CloneOf(schema)
			}

			for _, schemaDelta := range schemaDeltas {
				if schema, exists := schemasCopy[schemaDelta.DstTableName]; exists {
					columnNames := make(map[string]struct{}, len(schema.GetColumns()))
					for _, col := range schema.GetColumns() {
						columnNames[col.Name] = struct{}{}
					}
					for _, newCol := range schemaDelta.GetAddedColumns() {
						// only add columns that don't already exist
						if _, exists := columnNames[newCol.Name]; !exists {
							schema.Columns = append(schema.Columns, newCol)
							columnNames[newCol.Name] = struct{}{}
						} else {
							logger.Warn(fmt.Sprintf("skip adding duplicated column '%s' (type '%s') in table %s",
								newCol.Name, newCol.Type, schemaDelta.DstTableName))
						}
					}
				} else {
					logger.Warn(fmt.Sprintf("skip adding columns for table '%s' because it's not in catalog", schemaDelta.DstTableName))
				}
			}
			return schemasCopy, nil
		},
	); err != nil {
		return fmt.Errorf("failed to update table schemas in catalog: %w", err)
	}
	return nil
}
