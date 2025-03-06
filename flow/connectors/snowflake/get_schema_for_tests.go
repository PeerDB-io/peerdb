package connsnowflake

import (
	"context"
	"slices"

	"github.com/PeerDB-io/peerdb/flow/datatypes"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model/qvalue"
)

func (c *SnowflakeConnector) getTableSchemaForTable(ctx context.Context, tm *protos.TableMapping) (*protos.TableSchema, error) {
	columns, err := c.getColsFromTable(ctx, tm.SourceTableIdentifier)
	if err != nil {
		return nil, err
	}

	colFields := make([]*protos.FieldDescription, 0, len(columns))
	for _, sfColumn := range columns {
		if slices.Contains(tm.Exclude, sfColumn.ColumnName) {
			continue
		}

		genericColType, err := snowflakeTypeToQValueKind(sfColumn.ColumnType)
		if err != nil {
			// we use string for invalid types
			genericColType = qvalue.QValueKindString
		}

		colFields = append(colFields, &protos.FieldDescription{
			Name:         sfColumn.ColumnName,
			Type:         string(genericColType),
			TypeModifier: datatypes.MakeNumericTypmod(sfColumn.NumericPrecision, sfColumn.NumericScale),
		})
	}

	return &protos.TableSchema{
		TableIdentifier: tm.SourceTableIdentifier,
		Columns:         colFields,
		System:          protos.TypeSystem_Q,
	}, nil
}

// only used for testing atm. doesn't return info about pkey or ReplicaIdentity [which is PG specific anyway].
func (c *SnowflakeConnector) GetTableSchema(
	ctx context.Context,
	_env map[string]string,
	_system protos.TypeSystem,
	tableMappings []*protos.TableMapping,
) (map[string]*protos.TableSchema, error) {
	res := make(map[string]*protos.TableSchema, len(tableMappings))
	for _, tm := range tableMappings {
		tableSchema, err := c.getTableSchemaForTable(ctx, tm)
		if err != nil {
			return nil, err
		}
		res[tm.SourceTableIdentifier] = tableSchema
	}

	return res, nil
}
