package connmssql

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"slices"

	"github.com/PeerDB-io/peerdb/flow/connectors/postgres/sanitize"
	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/pkg/common"
	"github.com/PeerDB-io/peerdb/flow/pkg/mysql"
	"github.com/PeerDB-io/peerdb/flow/shared/datatypes"
)

func (c *MsSqlConnector) GetTableSchema(
	ctx context.Context,
	env map[string]string,
	version uint32,
	system protos.TypeSystem,
	tableMappings []*protos.TableMapping,
) (map[string]*protos.TableSchema, error) {
	res := make(map[string]*protos.TableSchema, len(tableMappings))
	for _, tm := range tableMappings {
		tableSchema, err := c.getTableSchemaForTable(ctx, env, tm, system)
		if err != nil {
			c.logger.Info("error fetching schema", slog.String("table", tm.SourceTableIdentifier), slog.Any("error", err))
			return nil, err
		}
		res[tm.SourceTableIdentifier] = tableSchema
		c.logger.Info("fetched schema", slog.String("table", tm.SourceTableIdentifier))
	}
	return res, nil
}

func (c *MsSqlConnector) getTableSchemaForTable(
	ctx context.Context,
	env map[string]string,
	tm *protos.TableMapping,
	system protos.TypeSystem,
) (*protos.TableSchema, error) {
	qualifiedTable, err := common.ParseTableIdentifier(tm.SourceTableIdentifier)
	if err != nil {
		return nil, err
	}

	nullableEnabled, err := internal.PeerDBNullable(ctx, env)
	if err != nil {
		return nil, err
	}

	rows, err := c.conn.QueryContext(ctx, fmt.Sprintf(`
		SELECT c.COLUMN_NAME, c.DATA_TYPE, c.IS_NULLABLE,
			COALESCE(c.NUMERIC_PRECISION, 0), COALESCE(c.NUMERIC_SCALE, 0),
			kcu.ORDINAL_POSITION AS pk_ordinal
		FROM INFORMATION_SCHEMA.COLUMNS c
		LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
			ON tc.TABLE_SCHEMA = c.TABLE_SCHEMA AND tc.TABLE_NAME = c.TABLE_NAME
			AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
		LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
			ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
			AND kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA AND kcu.TABLE_NAME = tc.TABLE_NAME
			AND kcu.COLUMN_NAME = c.COLUMN_NAME
		WHERE c.TABLE_SCHEMA = %s AND c.TABLE_NAME = %s
		ORDER BY c.ORDINAL_POSITION`,
		sanitize.QuoteString(qualifiedTable.Namespace), sanitize.QuoteString(qualifiedTable.Table)))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []*protos.FieldDescription
	type pkEntry struct {
		name    string
		ordinal int
	}
	var primaryEntries []pkEntry

	for rows.Next() {
		var columnName, dataType, isNullable string
		var numericPrecision, numericScale int64
		var pkOrdinal *int
		if err := rows.Scan(&columnName, &dataType, &isNullable, &numericPrecision, &numericScale, &pkOrdinal); err != nil {
			return nil, err
		}

		if slices.Contains(tm.Exclude, columnName) {
			continue
		}

		qkind, err := QkindFromMssqlColumnType(dataType)
		if err != nil {
			return nil, err
		}

		column := &protos.FieldDescription{
			Name:         columnName,
			Type:         string(qkind),
			TypeModifier: datatypes.MakeNumericTypmod(int32(numericPrecision), int32(numericScale)),
			Nullable:     isNullable == "YES",
		}
		columns = append(columns, column)

		if pkOrdinal != nil {
			primaryEntries = append(primaryEntries, pkEntry{name: columnName, ordinal: *pkOrdinal})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	slices.SortFunc(primaryEntries, func(a, b pkEntry) int {
		return cmp.Compare(a.ordinal, b.ordinal)
	})
	primary := make([]string, len(primaryEntries))
	for i, e := range primaryEntries {
		primary[i] = e.name
	}

	return &protos.TableSchema{
		TableIdentifier:       tm.SourceTableIdentifier,
		PrimaryKeyColumns:     primary,
		IsReplicaIdentityFull: false,
		System:                system,
		NullableEnabled:       nullableEnabled,
		Columns:               columns,
	}, nil
}

func (c *MsSqlConnector) GetAllTables(ctx context.Context) (*protos.AllTablesResponse, error) {
	rows, err := c.conn.QueryContext(ctx, `
		SELECT s.name + '.' + o.name
		FROM sys.objects o
		JOIN sys.schemas s ON o.schema_id = s.schema_id
		WHERE o.type = 'U' AND o.is_ms_shipped = 0
		ORDER BY s.name, o.name`)
	if err != nil {
		return nil, fmt.Errorf("[mssql] GetAllTables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables = append(tables, name)
	}
	return &protos.AllTablesResponse{Tables: tables}, rows.Err()
}

func (c *MsSqlConnector) GetSchemas(ctx context.Context) (*protos.PeerSchemasResponse, error) {
	rows, err := c.conn.QueryContext(ctx, `
		SELECT SCHEMA_NAME
		FROM INFORMATION_SCHEMA.SCHEMATA
		WHERE SCHEMA_NAME NOT IN ('sys', 'INFORMATION_SCHEMA', 'cdc', 'guest', 'db_owner',
			'db_accessadmin', 'db_securityadmin', 'db_ddladmin', 'db_backupoperator',
			'db_datareader', 'db_datawriter', 'db_denydatareader', 'db_denydatawriter')
		ORDER BY SCHEMA_NAME`)
	if err != nil {
		return nil, fmt.Errorf("[mssql] GetSchemas: %w", err)
	}
	defer rows.Close()

	var schemas []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		schemas = append(schemas, name)
	}
	return &protos.PeerSchemasResponse{Schemas: schemas}, rows.Err()
}

func (c *MsSqlConnector) GetTablesInSchema(
	ctx context.Context, schema string, cdcEnabled bool,
) (*protos.SchemaTablesResponse, error) {
	query := `
		SELECT t.TABLE_NAME,
			COALESCE(SUM(ps.used_page_count) * 8 * 1024, 0) AS size_bytes
		FROM INFORMATION_SCHEMA.TABLES t
		LEFT JOIN sys.dm_db_partition_stats ps
			ON ps.object_id = OBJECT_ID(t.TABLE_SCHEMA + '.' + t.TABLE_NAME)
			AND ps.index_id IN (0, 1)`
	if cdcEnabled {
		query += `
		INNER JOIN cdc.change_tables ct
			ON ct.source_object_id = OBJECT_ID(t.TABLE_SCHEMA + '.' + t.TABLE_NAME)`
	}
	query += "\n\t\tWHERE t.TABLE_SCHEMA = " + sanitize.QuoteString(schema) + //nolint:gosec // quoted
		" AND t.TABLE_TYPE = 'BASE TABLE'\n\t\tGROUP BY t.TABLE_NAME\n\t\tORDER BY t.TABLE_NAME"

	rows, err := c.conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("[mssql] GetTablesInSchema: %w", err)
	}
	defer rows.Close()

	var tables []*protos.TableResponse
	for rows.Next() {
		var tableName string
		var sizeBytes int64
		if err := rows.Scan(&tableName, &sizeBytes); err != nil {
			return nil, err
		}
		tables = append(tables, &protos.TableResponse{
			TableName: tableName,
			CanMirror: true,
			TableSize: mysql.PrettyBytes(sizeBytes),
		})
	}
	return &protos.SchemaTablesResponse{Tables: tables}, rows.Err()
}

func (c *MsSqlConnector) GetColumns(
	ctx context.Context, version uint32, schema string, table string,
) (*protos.TableColumnsResponse, error) {
	rows, err := c.conn.QueryContext(ctx, fmt.Sprintf(`
		SELECT c.COLUMN_NAME, c.DATA_TYPE,
			CASE WHEN kcu.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS is_pk
		FROM INFORMATION_SCHEMA.COLUMNS c
		LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
			ON tc.TABLE_SCHEMA = c.TABLE_SCHEMA AND tc.TABLE_NAME = c.TABLE_NAME
			AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
		LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
			ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
			AND kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA AND kcu.TABLE_NAME = tc.TABLE_NAME
			AND kcu.COLUMN_NAME = c.COLUMN_NAME
		WHERE c.TABLE_SCHEMA = %s AND c.TABLE_NAME = %s
		ORDER BY c.ORDINAL_POSITION`,
		sanitize.QuoteString(schema), sanitize.QuoteString(table)))
	if err != nil {
		return nil, fmt.Errorf("[mssql] GetColumns: %w", err)
	}
	defer rows.Close()

	var columns []*protos.ColumnsItem
	for rows.Next() {
		var name, dataType string
		var isPk bool
		if err := rows.Scan(&name, &dataType, &isPk); err != nil {
			return nil, err
		}
		qkind, err := QkindFromMssqlColumnType(dataType)
		if err != nil {
			return nil, err
		}
		columns = append(columns, &protos.ColumnsItem{
			Name:  name,
			Type:  dataType,
			IsKey: isPk,
			Qkind: string(qkind),
		})
	}
	return &protos.TableColumnsResponse{Columns: columns}, rows.Err()
}
