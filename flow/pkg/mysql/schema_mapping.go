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
	mapping := SchemaMapping{
		Columns:     make(map[common.QualifiedTable][]string),
		PrimaryKeys: make(map[common.QualifiedTable][]string),
	}
	if len(tables) == 0 {
		return mapping, nil
	}

	// CASTs are used in this query to ensure that we don't combine columns for tables with the
	// same name but different letter casings.
	where := fmt.Sprintf(
		"WHERE TABLE_SCHEMA = ?"+
			" AND CAST(TABLE_SCHEMA AS BINARY) = CAST(? AS BINARY)"+
			" AND CAST(TABLE_NAME AS BINARY) IN (CAST(? AS BINARY)%s)",
		strings.Repeat(",CAST(? AS BINARY)", len(tables)-1))

	params := make([]any, 0, 2+len(tables))
	params = append(params, schema, schema)
	for _, table := range tables {
		params = append(params, table)
	}

	// Both queries select (TABLE_NAME, COLUMN_NAME), grouped by the name the server returned so
	// that tables differing only in casing stay apart.
	readInto := func(dst map[common.QualifiedTable][]string, query string) error {
		rs, err := conn.Execute(query, params...)
		if err != nil {
			return err
		}
		defer rs.Close()

		for _, row := range rs.Values {
			if len(row) != 2 {
				return fmt.Errorf("expected 2 columns, got %d", len(row))
			}
			key := common.QualifiedTable{Namespace: schema, Table: string(row[0].AsString())}
			dst[key] = append(dst[key], string(row[1].AsString()))
		}
		return nil
	}

	// These two queries must stay un-joined. Before MySQL 8.0.3 the server prunes its
	// information_schema scan using WHERE-clause constants only, never a JOIN's ON clause, so a
	// joined read scans every database and opens tables in full rather than just their definitions.
	if err := readInto(mapping.Columns,
		"SELECT TABLE_NAME, COLUMN_NAME FROM information_schema.columns "+where+
			" ORDER BY TABLE_NAME, ORDINAL_POSITION"); err != nil {
		return SchemaMapping{}, fmt.Errorf("failed to list columns for schema %s: %w", schema, err)
	}

	// We opt for statistics instead of using columns.COLUMN_KEY because the latter also reports PRI for a
	// UNIQUE NOT NULL column on a table with no primary key.
	if err := readInto(mapping.PrimaryKeys,
		"SELECT TABLE_NAME, COLUMN_NAME FROM information_schema.statistics "+where+
			" AND INDEX_NAME = 'PRIMARY' ORDER BY TABLE_NAME, SEQ_IN_INDEX"); err != nil {
		return SchemaMapping{}, fmt.Errorf("failed to list primary keys for schema %s: %w", schema, err)
	}

	return mapping, nil
}
