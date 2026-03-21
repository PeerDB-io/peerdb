package connmysql

import (
	"context"
	"fmt"
	"slices"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/pkg/mysql"
)

func (c *MySqlConnector) GetAllTables(ctx context.Context) (*protos.AllTablesResponse, error) {
	rs, err := c.Execute(ctx, `
		SELECT concat(table_schema, '.', table_name)
		FROM information_schema.tables
		WHERE table_schema NOT IN ('information_schema', 'performance_schema', 'sys')
		  AND table_type = 'BASE TABLE'`)
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
	rs, err := c.Execute(ctx, fmt.Sprintf(`
		SELECT table_name, data_length + index_length
		FROM information_schema.tables
		WHERE table_schema = '%s'
		  AND table_type = 'BASE TABLE'
		ORDER BY table_name`, gomysql.Escape(schema)))
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

func (c *MySqlConnector) GetColumns(ctx context.Context, version uint32, schema string, table string) (*protos.TableColumnsResponse, error) {
	rs, err := c.Execute(ctx, fmt.Sprintf(`
		SELECT column_name, column_type, column_key
		FROM information_schema.columns
		WHERE table_schema = '%s'
		  AND table_name = '%s'
		ORDER BY column_name`,
		gomysql.Escape(schema), gomysql.Escape(table)))
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
		qkind, err := QkindFromMysqlColumnType(columnType, version)
		if err != nil {
			return nil, err
		}
		columns = append(columns, &protos.ColumnsItem{
			Name:  columnName,
			Type:  columnType,
			IsKey: columnKey == "PRI",
			Qkind: string(qkind),
		})
	}
	return &protos.TableColumnsResponse{Columns: columns}, nil
}
