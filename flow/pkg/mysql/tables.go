package mysql

import (
	"fmt"

	"github.com/go-mysql-org/go-mysql/client"
)

type ColumnInfo struct {
	Name string
	// DataType is information_schema.COLUMN.DATA_TYPE, e.g. "varchar", "int".
	DataType string
	// ColumnType is information_schema.COLUMN.COLUMN_TYPE, e.g. "varchar(100)", "int unsigned".
	// This is the value MySQL type mapping (QkindFromMysqlColumnType) expects.
	ColumnType       string
	IsNullable       bool
	NumericPrecision int32
	NumericScale     int32
}

type TableInfo struct {
	Name      string
	SizeBytes int64
	// PrimaryKey lists the primary-key columns in key order (by SEQ_IN_INDEX); empty if none.
	PrimaryKey []string
	Columns    []ColumnInfo
}

// GetTablesBySchema returns the base tables in the given schema, each with its on-disk size
// (data + index length), primary key (in key order) and columns (in ordinal order).
func GetTablesBySchema(conn *client.Conn, schema string) ([]TableInfo, error) {
	sizeRs, err := conn.Execute(
		`SELECT TABLE_NAME, COALESCE(DATA_LENGTH + INDEX_LENGTH, 0)
		FROM information_schema.tables
		WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'`, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to query table sizes for schema %s: %w", schema, err)
	}

	sizeByTable := make(map[string]int64, len(sizeRs.Values))
	for _, row := range sizeRs.Values {
		sizeByTable[string(row[0].AsString())] = row[1].AsInt64()
	}

	pkRs, err := conn.Execute(
		`SELECT TABLE_NAME, COLUMN_NAME
		FROM information_schema.statistics
		WHERE TABLE_SCHEMA = ? AND INDEX_NAME = 'PRIMARY'
		ORDER BY SEQ_IN_INDEX`, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to query primary keys for schema %s: %w", schema, err)
	}
	pkByTable := make(map[string][]string)
	for _, row := range pkRs.Values {
		tableName := string(row[0].AsString())
		pkByTable[tableName] = append(pkByTable[tableName], string(row[1].AsString()))
	}

	colRs, err := conn.Execute(
		`SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE,
			NUMERIC_PRECISION, NUMERIC_SCALE
		FROM information_schema.columns
		WHERE TABLE_SCHEMA = ?
		ORDER BY TABLE_NAME, ORDINAL_POSITION`, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns for schema %s: %w", schema, err)
	}

	tables := make([]TableInfo, 0, len(sizeByTable))
	indexByTable := make(map[string]int, len(sizeByTable))
	for i := range colRs.RowNumber() {
		row := colRs.Values[i]
		tableName := string(row[0].AsString())
		size, isBaseTable := sizeByTable[tableName]
		if !isBaseTable {
			continue
		}
		precision, err := colRs.GetInt(i, 5)
		if err != nil {
			return nil, fmt.Errorf("failed to read numeric precision for %s.%s: %w", schema, tableName, err)
		}
		scale, err := colRs.GetInt(i, 6)
		if err != nil {
			return nil, fmt.Errorf("failed to read numeric scale for %s.%s: %w", schema, tableName, err)
		}
		idx, seen := indexByTable[tableName]
		if !seen {
			idx = len(tables)
			indexByTable[tableName] = idx
			tables = append(tables, TableInfo{
				Name:       tableName,
				SizeBytes:  size,
				PrimaryKey: pkByTable[tableName],
			})
		}
		tables[idx].Columns = append(tables[idx].Columns, ColumnInfo{
			Name:             string(row[1].AsString()),
			DataType:         string(row[2].AsString()),
			ColumnType:       string(row[3].AsString()),
			IsNullable:       string(row[4].AsString()) == "YES",
			NumericPrecision: int32(precision),
			NumericScale:     int32(scale),
		})
	}

	return tables, nil
}
