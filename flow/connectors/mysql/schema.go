package connmysql

import (
	"context"
	"slices"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/shared/mysql"
)

func (c *MySqlConnector) GetAllTables(ctx context.Context) (*protos.AllTablesResponse, error) {
	rs, err := c.Execute(ctx, `select concat(table_schema, '.', table_name) from information_schema.tables
		where table_schema not in ('information_schema', 'performance_schema', 'sys')`)
	if err != nil {
		return nil, err
	}

	tables := make([]string, 0, rs.RowNumber())
	for idx := range rs.RowNumber() {
		val, err := rs.GetString(idx, 0)
		if err != nil {
			return nil, err
		}
		tables = append(tables, val)
	}
	return &protos.AllTablesResponse{Tables: tables}, nil
}

func (c *MySqlConnector) GetSchemas(ctx context.Context) (*protos.PeerSchemasResponse, error) {
	rs, err := c.Execute(ctx, "SHOW SCHEMAS")
	if err != nil {
		return nil, err
	}

	schemas := make([]string, 0, rs.RowNumber())
	for idx := range rs.RowNumber() {
		val, err := rs.GetString(idx, 0)
		if err != nil {
			return nil, err
		}
		if !slices.Contains([]string{"information_schema", "performance_schema", "sys"}, val) {
			schemas = append(schemas, val)
		}
	}
	return &protos.PeerSchemasResponse{Schemas: schemas}, nil
}

func (c *MySqlConnector) GetTablesInSchema(
	ctx context.Context, schema string, cdcEnabled bool,
) (*protos.SchemaTablesResponse, error) {
	rs, err := c.Execute(ctx, `select table_name, data_length + index_length
		from information_schema.tables where table_schema = ? order by table_name`, schema)
	if err != nil {
		return nil, err
	}

	tables := make([]*protos.TableResponse, 0, rs.RowNumber())
	for idx := range rs.RowNumber() {
		tableName, err := rs.GetString(idx, 0)
		if err != nil {
			return nil, err
		}
		tableSizeInBytes, err := rs.GetInt(idx, 1)
		if err != nil {
			return nil, err
		}
		tables = append(tables, &protos.TableResponse{
			TableName: tableName,
			CanMirror: true,
			TableSize: mysql.PrettyBytes(tableSizeInBytes),
		})
	}
	return &protos.SchemaTablesResponse{Tables: tables}, nil
}

func (c *MySqlConnector) GetColumns(ctx context.Context, schema string, table string) (*protos.TableColumnsResponse, error) {
	rs, err := c.Execute(ctx, `select column_name, column_type, column_key
		from information_schema.columns where table_schema = ? and table_name = ? order by column_name`,
		schema, table)
	if err != nil {
		return nil, err
	}

	columns := make([]*protos.ColumnsItem, 0, rs.RowNumber())
	for idx := range rs.RowNumber() {
		columnName, err := rs.GetString(idx, 0)
		if err != nil {
			return nil, err
		}
		columnType, err := rs.GetString(idx, 1)
		if err != nil {
			return nil, err
		}
		columnKey, err := rs.GetString(idx, 2)
		if err != nil {
			return nil, err
		}
		columns = append(columns, &protos.ColumnsItem{
			Name:  columnName,
			Type:  columnType,
			IsKey: columnKey == "PRI",
		})
	}
	return &protos.TableColumnsResponse{Columns: columns}, nil
}
