package connsnowflake

import (
	"context"

	"github.com/PeerDB-io/peer-flow/datatypes"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

func (c *SnowflakeConnector) getTableSchemaForTable(ctx context.Context, tableName string) (*protos.TableSchema, error) {
	columns, err := c.getColsFromTable(ctx, tableName)
	if err != nil {
		return nil, err
	}

	colFields := make([]*protos.FieldDescription, 0, len(columns))
	for i, sfColumn := range columns {
		genericColType, err := snowflakeTypeToQValueKind(sfColumn.ColumnType)
		if err != nil {
			// we use string for invalid types
			genericColType = qvalue.QValueKindString
		}

		colFields = append(colFields, &protos.FieldDescription{
			Name:         columns[i].ColumnName,
			Type:         string(genericColType),
			TypeModifier: datatypes.MakeNumericTypmod(sfColumn.NumericPrecision, sfColumn.NumericScale),
		})
	}

	return &protos.TableSchema{
		TableIdentifier: tableName,
		Columns:         colFields,
		System:          protos.TypeSystem_Q,
	}, nil
}

// only used for testing atm. doesn't return info about pkey or ReplicaIdentity [which is PG specific anyway].
func (c *SnowflakeConnector) GetTableSchema(
	ctx context.Context,
	_env map[string]string,
	_system protos.TypeSystem,
	tableIdentifiers []string,
) (map[string]*protos.TableSchema, error) {
	res := make(map[string]*protos.TableSchema, len(tableIdentifiers))
	for _, tableName := range tableIdentifiers {
		tableSchema, err := c.getTableSchemaForTable(ctx, tableName)
		if err != nil {
			return nil, err
		}
		res[tableName] = tableSchema
	}

	return res, nil
}
