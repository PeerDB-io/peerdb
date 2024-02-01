package connsnowflake

import (
	"fmt"

	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/model/qvalue"
)

func (c *SnowflakeConnector) getTableSchemaForTable(tableName string) (*protos.TableSchema, error) {
	colNames, colTypes, err := c.getColsFromTable(tableName)
	if err != nil {
		return nil, err
	}

	colFields := make([]*protos.FieldDescription, 0, len(colNames))
	for i, sfType := range colTypes {
		genericColType, err := snowflakeTypeToQValueKind(sfType)
		if err != nil {
			// we use string for invalid types
			genericColType = qvalue.QValueKindString
		}
		colTypes[i] = string(genericColType)
		colFields = append(colFields, &protos.FieldDescription{
			ColumnName:   colNames[i],
			ColumnType:   colTypes[i],
			TypeModifier: -1,
		})
	}

	return &protos.TableSchema{
		TableIdentifier: tableName,
		Columns:         colFields,
	}, nil
}

// only used for testing atm. doesn't return info about pkey or ReplicaIdentity [which is PG specific anyway].
func (c *SnowflakeConnector) GetTableSchema(
	req *protos.GetTableSchemaBatchInput,
) (*protos.GetTableSchemaBatchOutput, error) {
	res := make(map[string]*protos.TableSchema, len(req.TableIdentifiers))
	for _, tableName := range req.TableIdentifiers {
		tableSchema, err := c.getTableSchemaForTable(tableName)
		if err != nil {
			return nil, err
		}
		res[tableName] = tableSchema
		utils.RecordHeartbeat(c.ctx, fmt.Sprintf("fetched schema for table %s", tableName))
	}

	return &protos.GetTableSchemaBatchOutput{
		TableNameSchemaMapping: res,
	}, nil
}
