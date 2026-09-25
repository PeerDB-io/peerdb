package mysql

import (
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/client"

	"github.com/PeerDB-io/peerdb/flow/pkg/common"
)

// SchemaMapping holds the columns (ORDINAL_POSITION order) and primary-key columns
// (SEQ_IN_INDEX order) of tables in one schema. No primary key means no PrimaryKeys entry.
type SchemaMapping struct {
	Columns     map[common.QualifiedTable][]string
	PrimaryKeys map[common.QualifiedTable][]string
}

// GetSchemaMapping returns the columns and primary-key columns of the named tables.
func GetSchemaMapping(conn *client.Conn, schema string, tables []string) (SchemaMapping, error) {
	if len(tables) == 0 {
		return SchemaMapping{
			Columns:     make(map[common.QualifiedTable][]string),
			PrimaryKeys: make(map[common.QualifiedTable][]string),
		}, nil
	}

	where, tableParams := infoSchemaPredicate(tables)

	// These two queries must stay un-joined. Before MySQL 8.0.3 the server prunes its
	// information_schema scan using WHERE-clause constants only, never a JOIN's ON clause, so a
	// joined read scans every database and opens tables in full rather than just their definitions.
	//
	// Measured on 120 schemas of 100 tables each, requesting 50 tables: these two queries read
	// 50 table definitions and open 0 tables, against 12118 and 12120 for the joined form.
	// Seconds against milliseconds of latency.
	columnRows, err := queryTableColumnPairs(conn,
		"SELECT TABLE_NAME, COLUMN_NAME FROM information_schema.columns"+where+
			" ORDER BY TABLE_NAME, ORDINAL_POSITION", schema, tableParams)
	if err != nil {
		// Keep the errors wrapped with %w to help the classifier identify the error.
		return SchemaMapping{}, fmt.Errorf("failed to list columns for schema %s: %w", schema, err)
	}

	// We opt for statistics instead of using columns.COLUMN_KEY because the latter also reports PRI for a
	// UNIQUE NOT NULL column on a table with no primary key. We also gain primary key ordering from this.
	pkRows, err := queryTableColumnPairs(conn,
		"SELECT TABLE_NAME, COLUMN_NAME FROM information_schema.statistics"+where+
			" AND INDEX_NAME = 'PRIMARY' ORDER BY TABLE_NAME, SEQ_IN_INDEX", schema, tableParams)
	if err != nil {
		return SchemaMapping{}, fmt.Errorf("failed to list primary keys for schema %s: %w", schema, err)
	}

	return foldSchemaMappingRows(schema, columnRows, pkRows), nil
}

// infoSchemaPredicate builds the WHERE clause shared by both reads.
func infoSchemaPredicate(tables []string) (string, []any) {
	params := make([]any, 0, len(tables))
	for _, table := range tables {
		params = append(params, table)
	}

	// The CASTs make the match byte-exact so case-variant tables are not conflated;
	// the plain TABLE_SCHEMA predicate is the one the 5.7 scan pruning can use for
	// higher performance.
	return fmt.Sprintf(
		" WHERE TABLE_SCHEMA = ?"+
			" AND CAST(TABLE_SCHEMA AS BINARY) = CAST(? AS BINARY)"+
			" AND CAST(TABLE_NAME AS BINARY) IN (CAST(? AS BINARY)%s)",
		strings.Repeat(",CAST(? AS BINARY)", len(tables)-1)), params
}

func queryTableColumnPairs(conn *client.Conn, query string, schema string, tableParams []any) ([][2]string, error) {
	params := make([]any, 0, 2+len(tableParams))
	params = append(params, schema, schema)
	params = append(params, tableParams...)

	rs, err := conn.Execute(query, params...)
	if err != nil {
		return nil, err
	}
	defer rs.Close()

	rows := make([][2]string, 0, len(rs.Values))
	for _, row := range rs.Values {
		if len(row) != 2 {
			return nil, fmt.Errorf("expected 2 columns, got %d", len(row))
		}
		rows = append(rows, [2]string{string(row[0].AsString()), string(row[1].AsString())})
	}
	return rows, nil
}

func foldSchemaMappingRows(schema string, columnRows, pkRows [][2]string) SchemaMapping {
	result := SchemaMapping{
		Columns:     make(map[common.QualifiedTable][]string),
		PrimaryKeys: make(map[common.QualifiedTable][]string),
	}
	for _, row := range columnRows {
		key := common.QualifiedTable{Namespace: schema, Table: row[0]}
		result.Columns[key] = append(result.Columns[key], row[1])
	}
	for _, row := range pkRows {
		key := common.QualifiedTable{Namespace: schema, Table: row[0]}
		result.PrimaryKeys[key] = append(result.PrimaryKeys[key], row[1])
	}
	return result
}
